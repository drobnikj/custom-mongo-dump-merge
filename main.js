const Apify = require('apify');
const MongoClient = require('mongodb').MongoClient;
const Promise = require('bluebird');
const _ = require('underscore');

Apify.main(async () => {
    const { mainDBUrl, dumpDBUrl, fromId, processedOffers } = await Apify.getValue('INPUT');

    const state = await Apify.getValue('STATE') || {
        fromId: fromId || '000000000000000',
        processedOffers: processedOffers || 0,
        modifiedAt: new Date(),
        offersToMerge: [],
        offersWithMultipleDuplicates: [],
    };


    const mainDbClient = await MongoClient.connect(mainDBUrl);
    const dumpDbClient = await MongoClient.connect(dumpDBUrl);

    const maindb = await mainDbClient.db('realist-mongo-merged');
    const dumpdb = await dumpDbClient.db('realist-db');

    const fields = { _id: 1, siteId: 1, localUniqueId: 1 };

    while (true) {
        console.log(`Processing offers from ${state.fromId}, processedOffers: ${state.processedOffers}.`);
        const offersToProcess = await dumpdb.collection('offers')
            .find({ _id: { $gt: state.fromId } }, { limit: 1000, sort: { _id: 1 }, fields })
            .toArray();
        const offersToProcessIds = _.indexBy(offersToProcess, '_id');
        const lastId = Object.keys(_.indexBy(offersToProcess, '_id')).pop();
        const processingOffersCount = Object.keys(offersToProcessIds).length;
        const offersToMerge = [];

        if (processingOffersCount === 0) break;

        // console.log('1. ', JSON.stringify(Object.keys(offersToProcessIds)));
        // Remove existing offers
        await maindb.collection('offers').find({ _id: { $in: Object.keys(offersToProcessIds) } }, { fields }).forEach((offer) => {
            delete offersToProcessIds[offer._id];
        });

        console.log(`After remove existing -> Offers to import: ${Object.keys(offersToProcessIds).length} from ${processingOffersCount}`);
        // console.log('2. ', JSON.stringify(Object.keys(offersToProcessIds)));

        // Remove existing by siteId + uniqueId
        const query = {
            $or: [],
        };
        const quickFindBySiteLocalId = {};
        Object.values(offersToProcessIds).forEach((offer) => {
            query.$or.push({ siteId: offer.siteId, localUniqueId: offer.localUniqueId });
            quickFindBySiteLocalId[`${offer.siteId}_${offer.localUniqueId}`] = offer._id;
        });
        if (query.$or.length) {
            await maindb.collection('offers').find(query, { fields }).forEach((offer) => {
                const { siteId, localUniqueId } = offer;
                offersToMerge.push(offer);
                const offerId = quickFindBySiteLocalId[`${siteId}_${localUniqueId}`];
                delete offersToProcessIds[offerId];
            });
        }

        console.log(`After remove by siteId + uniqueId -> Offers to import: ${Object.keys(offersToProcessIds).length} from ${processingOffersCount}`);
        // console.log('3. ', JSON.stringify(Object.keys(offersToProcessIds)));

        // Import offers
        const duplicatesToImport = [];
        const offersToImport = await dumpdb.collection('offers')
            .find({ _id: { $in: Object.keys(offersToProcessIds) } }).toArray();

        console.time('offersToImport');
        // Check duplicates from v2 and fallback for old duplicates collection
        const duplicatesV2 = await dumpdb.collection('offerDuplicatesV2').find({ _id: { $in: Object.keys(offersToProcessIds) } }).toArray();
        const duplicatesV2ByKey = _.indexBy(duplicatesV2, '_id');
        for (const offerToImport of offersToImport) {
            const duplicatesV2ForOffer = duplicatesV2ByKey[offerToImport._id];

            if (duplicatesV2ForOffer) {
                duplicatesToImport.push(duplicatesV2ForOffer);
            } else {
                const duplicatesForOffer = await dumpdb.collection('offerDuplicates')
                    .find({ offerIds: { $in: [offerToImport._id] } }).toArray();
                if (duplicatesForOffer.length) {
                    const { offerIds, modifiedAt } = duplicatesForOffer[0];
                    delete offerIds[offerToImport._id];
                    duplicatesToImport.push({
                        _id: offerToImport._id,
                        duplicateOfferIds: offerIds,
                        modifiedAt,
                    });
                    if (duplicatesForOffer.length > 1) {
                        state.offersWithMultipleDuplicates.push(offerToImport._id);
                    }
                }
            }
        }
        console.timeEnd('offersToImport');

        // Imports offers and offerDuplicatesV2
        if (offersToImport.length) {
            console.log(`Importing offers: ${offersToImport.map(offer => offer._id).join(', ')}`);
            await maindb.collection('offers').insertMany(offersToImport);
        }
        if (duplicatesToImport.length) {
            console.log(`Importing duplicates offers: ${duplicatesToImport.map(duplicate => duplicate._id).join(', ')}`);
            await maindb.collection('offerDuplicatesV2').insertMany(duplicatesToImport);
        }

        // Update deleted offers(merge with current)
        const promises = [];
        for (let offerToUpdate of offersToMerge) {
            offerToUpdate = await maindb.collection('offers').findOne({ _id: offerToUpdate._id });
            const dumpOfferToMerge = await dumpdb.collection('offers').findOne({ siteId: offerToUpdate.siteId, localUniqueId: offerToUpdate.localUniqueId });
            console.log(dumpOfferToMerge.createdAt,offerToUpdate.createdAt);
            if (dumpOfferToMerge.createdAt.toString() === offerToUpdate.createdAt.toString()) {
                console.log(`Offer ${offerToUpdate._id} already updated!`);
            } else {
                const historyOffset = dumpOfferToMerge.metaHistory.length;
                const mergedDataHistory = dumpOfferToMerge.dataHistory.concat(offerToUpdate.dataHistory.map((data) => {
                    data.versionNumber += historyOffset;
                    return data;
                }));
                const mergedMetaHistory = dumpOfferToMerge.metaHistory.concat(offerToUpdate.metaHistory.map((meta) => {
                    meta.versionNumber += historyOffset;
                    return meta;
                }));
                const dataMerged = offerToUpdate.data;
                dataMerged.versionNumber = (mergedDataHistory.length) ? dataMerged.versionNumber + historyOffset : 0;
                const metaMerged = offerToUpdate.meta;
                metaMerged.versionNumber += historyOffset;
                // await Apify.setValue(offerToUpdate._id, { createdAt: dumpOfferToMerge.createdAt, mergedDataHistory, mergedMetaHistory, dataMerged, metaMerged });
                const modifier = {
                    $set: {
                        createdAt: dumpOfferToMerge.createdAt,
                        data: dataMerged,
                        meta: metaMerged,
                        dataHistory: mergedDataHistory,
                        metaHistory: mergedMetaHistory,
                    },
                };
                console.log(`Updating offer ${offerToUpdate._id}`);
                promises.push(maindb.collection('offers').update({ _id: offerToUpdate._id }, modifier));
            }
        }
        await Promise.all(promises);


        state.fromId = lastId;
        state.processedOffers += processingOffersCount;

        await Apify.setValue('STATE', state);
        console.log(`State saved ${JSON.stringify(_.omit(state, 'offersToMerge'))}`);
    }

    console.log('Done.');
});
