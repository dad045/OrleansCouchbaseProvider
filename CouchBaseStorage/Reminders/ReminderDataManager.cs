﻿#region

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.N1QL;
using CouchbaseProviders.CouchbaseComm;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Storage;

#endregion

namespace CouchbaseProviders.Reminders
{
    internal class ReminderDataManager : CouchbaseDataManager
    {
        private const string CollectionName = "orem";
        private readonly string _baseIdQuery;
        private readonly IGrainFactory _grainFactory;
        private readonly IGrainReferenceConverter _grainReferenceConverter;
        private readonly ILogger<ReminderDataManager> _logger;
        private List<CouchbaseReminderDocument> _entries;
        private DateTimeOffset _lastGetAllReminderEntries;
        private readonly ClusterOptions _clusterOptions;

        public ReminderDataManager(
            string bucketName,
            IBucketFactory bucketFactory,
            ILoggerFactory loggerFactory,
            IGrainFactory grainFactory,
            IGrainReferenceConverter grainReferenceConverter,
            ClusterOptions clusterOptions
        ) : base(bucketName, bucketFactory, clusterOptions)
        {
            BucketName = bucketName;
            _logger = loggerFactory.CreateLogger<ReminderDataManager>();
            _grainFactory = grainFactory;
            _grainReferenceConverter = grainReferenceConverter;
            _clusterOptions = clusterOptions;
            if(!string.IsNullOrEmpty(clusterOptions?.ClusterId))
                _baseIdQuery = $"select meta().id from {BucketName} where docSubType = \"{DocSubTypes.Reminder}\" and clusterId = \"{clusterOptions.ClusterId}\" ";
            else
                _baseIdQuery = $"select meta().id from {BucketName} where docSubType = \"{DocSubTypes.Reminder}\" ";
        }

        public async Task<bool> DeleteReminder(GrainReference grainRef, string reminderName, string eTag, bool forceDelete = false)
        {
            try
            {
                await Delete(CollectionName, reminderName, eTag);
                //todo remove this try catch when reminder deletion investigation is complete
                try
                {
                    throw new Exception("Delete Reminder called! Reminders are getting mysteriously deleted.  This is intended to help with that investigation. ");
                } 
                catch(Exception ex)
                {
                    _logger.LogError(ex,ex.Message);
                    Console.WriteLine(ex.Message);
                    Console.WriteLine(ex.StackTrace);
                }
                return true;
            }
            catch (InconsistentStateException e)
            {
                Console.WriteLine(e);
                if (!forceDelete)
                {
                    return false;
                }
            }

            IOperationResult deleteResult = new OperationResult();
            IDocumentResult<ReminderEntry> getResult = await Bucket.GetDocumentAsync<ReminderEntry>(reminderName);
            if (getResult.Success)
            {
                deleteResult = await Bucket.RemoveAsync(getResult.Document);
            }

            return deleteResult.Success;
        }

        public async Task<ReminderTableData> GetAllReminderRows()
        {
            List<CouchbaseReminderDocument> docs = await GetAllReminderEntries();
            IEnumerable<ReminderEntry> entries = CouchbaseReminderDocument.ToReminderEntries(docs, _grainReferenceConverter);
            return new ReminderTableData(entries);
        }


        public async Task<ReminderEntry> ReadReminder(GrainReference grainRef, string reminderName)
        {
            CouchbaseReminderDocument reminderDoc = null;
            (string reminderDocumentString, string cas) = await Read(CollectionName, reminderName);
            try
            {
                if(reminderDocumentString != null )
                    reminderDoc = JsonConvert.DeserializeObject<CouchbaseReminderDocument>(reminderDocumentString);
            }
            catch (JsonException e)
            {
                Console.WriteLine(e);
                throw;
            }

            if (reminderDoc == null) return null;
            
            if(!string.IsNullOrEmpty(_clusterOptions.ClusterId) || !string.IsNullOrEmpty(reminderDoc.ClusterId))
            {
                if (string.IsNullOrEmpty(reminderDoc.ClusterId) || string.IsNullOrEmpty(_clusterOptions.ClusterId) || !reminderDoc.ClusterId.Equals(_clusterOptions.ClusterId))
                    return null;
            }

            ReminderEntry reminder = reminderDoc.ToReminderEntry(_grainReferenceConverter);
            reminder.ETag = cas;
            return reminder;
        }

        public async Task<ReminderTableData> ReadRowsInRange(uint begin, uint end)
        {
            var entries = new List<CouchbaseReminderDocument>();
            var query = new QueryRequest($"{_baseIdQuery} and grainHash > {begin} and grainHash < {end}");
            List<string> idStrings = await QueryForIdStrings(query);
            await GetAllRemindersFromIdList(idStrings, entries);
            IEnumerable<ReminderEntry> newList = CouchbaseReminderDocument.ToReminderEntries(entries, _grainReferenceConverter);
            return new ReminderTableData(newList);
        }

        public async Task<ReminderTableData> ReadRowsOutRange(uint begin, uint end)
        {
            var entries = new List<CouchbaseReminderDocument>();
            var query = new QueryRequest($"{_baseIdQuery} and grainHash < {begin} and grainHash > {end}");
            List<string> idStrings = await QueryForIdStrings(query);
            await GetAllRemindersFromIdList(idStrings, entries);
            IEnumerable<ReminderEntry> newList = CouchbaseReminderDocument.ToReminderEntries(entries, _grainReferenceConverter);
            return new ReminderTableData(newList);
        }

        public async Task TestOnlyClearTable(string bucketName)
        {
//            var deleteQuery = new QueryRequest($"delete from {bucketName} where docSubType = \"{DocSubTypes.Reminder}\"");
//            deleteQuery.ScanConsistency(ScanConsistency.RequestPlus);
//            deleteQuery.Metrics(false);
//            IQueryResult<JObject> ids = await Bucket.QueryAsync<JObject>(deleteQuery).ConfigureAwait(false);
        }


        public async Task<CouchbaseReminderDocument> UpdateReminder(ReminderEntry entry)
        {
            var doc = new CouchbaseReminderDocument(entry, entry.ETag, _clusterOptions.ClusterId);
            string result = await WriteAsync(CollectionName, doc.ReminderName, doc, doc.ETag);
            doc.ETag = result;
            return doc;
        }

        private async Task<List<CouchbaseReminderDocument>> GetAllReminderEntries()
        {
            TimeSpan lastGet = DateTimeOffset.UtcNow - _lastGetAllReminderEntries;
            if (lastGet <= TimeSpan.FromMilliseconds(500))
            {
                return _entries;
            }

            var entries = new List<CouchbaseReminderDocument>();
            var readAllQuery = new QueryRequest($"{_baseIdQuery}");
            List<string> idStrings = await QueryForIdStrings(readAllQuery);

            await GetAllRemindersFromIdList(idStrings, entries);

            _lastGetAllReminderEntries = DateTimeOffset.UtcNow;
            _entries = entries;
            return entries;
        }

        private async Task GetAllRemindersFromIdList(IList<string> idStrings, ICollection<CouchbaseReminderDocument> entries)
        {
            foreach (string id in idStrings)
            {
                IOperationResult<CouchbaseReminderDocument> result = await Bucket.GetAsync<CouchbaseReminderDocument>(id, TimeSpan.FromSeconds(10));
                if (result.Success)
                {
                    CouchbaseReminderDocument reminder = result.Value;
                    reminder.ETag = result.Cas.ToString();
                    _grainFactory.BindGrainReference(reminder.GrainRef);
                    entries.Add(reminder);
                    continue;
                }
                _logger.LogWarning($"Unable to retrieve reminder document from couchbase while an id was found. {id} position: {idStrings.IndexOf(id)} total:{idStrings.Count}");
                _logger.LogDebug($"Response from Bucket GetAsync: \n{JsonConvert.SerializeObject(result)}");
            }
        }

        private async Task<List<string>> QueryForIdStrings(QueryRequest readAllQuery)
        {
            readAllQuery.ScanConsistency(ScanConsistency.RequestPlus);
            readAllQuery.Metrics(false);
            IQueryResult<JObject> ids = await Bucket.QueryAsync<JObject>(readAllQuery).ConfigureAwait(false);

            List<string> idStrings = ids.Rows.Select(x => x["id"].ToString()).ToList();
            return idStrings;
        }
    }
}