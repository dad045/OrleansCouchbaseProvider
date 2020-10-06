#region

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Configuration.Client;
using Couchbase.Configuration.Server.Serialization;
using Couchbase.N1QL;
using CouchbaseProviders.CouchbaseComm;
using CouchbaseProviders.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Runtime;

#endregion

// ReSharper disable CheckNamespace
// ReSharper disable ClassNeverInstantiated.Global

namespace Orleans.Storage
{
    public class CouchbaseMembershipProvider : IMembershipTable
    {
        private readonly CouchbaseProvidersSettings _couchbaseProvidersSettings;
        public readonly string BucketName;


        private readonly IBucketFactory _bucketFactory;
        private readonly ILogger<CouchbaseMembershipProvider> _logger;
        private MembershipDataManager _manager;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IOptions<ClusterOptions> _clusterOptions;

        public CouchbaseMembershipProvider(ILogger<CouchbaseMembershipProvider> logger, IOptions<CouchbaseProvidersSettings> options, IBucketFactory bucketFactory, ILoggerFactory loggerFactory, IOptions<ClusterOptions> clusterOptions)
        {
            _logger = logger;
            _couchbaseProvidersSettings = options.Value;
            BucketName = _couchbaseProvidersSettings.MembershipBucketName;
            _bucketFactory = bucketFactory;
            _loggerFactory = loggerFactory;
            _clusterOptions = clusterOptions;
            _logger.LogWarning($"My MembershipBucket is: {BucketName}");
            _logger.LogWarning($"My ServiceId is: {clusterOptions.Value.ServiceId}");
            _logger.LogWarning($"My ClusterId is: {clusterOptions.Value.ClusterId}");
        }

        public Task DeleteMembershipTableEntries(string clusterId)
        {
            return _manager.DeleteMembershipTableEntries(clusterId);
        }


        public Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            try
            {
                _manager = new MembershipDataManager(_couchbaseProvidersSettings.MembershipBucketName, _bucketFactory, _loggerFactory, _clusterOptions.Value);
            }
            catch (BootstrapException e)
            {
                Console.WriteLine(e);
                _logger.LogError(e, "Error communicating with the database during init.  Killing Silo.  ");
                Process.GetCurrentProcess().Kill();
            }
            return Task.CompletedTask;
        }

        public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            return _manager.InsertRow(entry, tableVersion);
        }

        public Task<MembershipTableData> ReadAll()
        {
            return _manager.ReadAll();
        }

        public Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            return _manager.ReadRow(key);
        }

        public Task UpdateIAmAlive(MembershipEntry entry)
        {
            return _manager.UpdateIAmAlive(entry);
        }

        public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            return _manager.UpdateRow(entry, tableVersion, etag);
        }

        public Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            return _manager.CleanupDefunctSiloEntries(beforeDate);
        }
    }

    public class CouchbaseGatewayListProvider : IGatewayListProvider
    {
        private readonly IBucketFactory _bucketFactory;
        private MembershipDataManager _manager;
        private readonly CouchbaseProvidersSettings _settings;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ClusterOptions _clusterOptions;

        public CouchbaseGatewayListProvider(IBucketFactory bucketFactory, IOptions<CouchbaseProvidersSettings> settings, ILoggerFactory loggerFactory, IOptions<ClusterOptions> clusterOptions)
        {
            _settings = settings.Value;
            _bucketFactory = bucketFactory;
            _loggerFactory = loggerFactory;
            _clusterOptions = clusterOptions.Value;
        }

        public bool IsUpdatable
        {
            get { return true; }
        }

        public TimeSpan MaxStaleness { get; private set; }

        public Task<IList<Uri>> GetGateways()
        {
            return _manager.GetGateWays();
        }


        public async Task InitializeGatewayListProvider()
        {
            _manager = new MembershipDataManager(_settings.MembershipBucketName, _bucketFactory, _loggerFactory, _clusterOptions);

            MaxStaleness = _settings.RefreshRate;

            //todo do we need this anymore?
            await _manager.CleanupDefunctSiloEntries();
        }
    }

    public class MembershipDataManager : CouchbaseDataManager
    {
        private readonly TableVersion _tableVersion = new TableVersion(0, "0");
        private readonly ILogger<MembershipDataManager> _logger;
        private readonly ClusterOptions _clusterOptions;

        public MembershipDataManager(string bucketName, ClientConfiguration clientConfig) : base(bucketName, clientConfig)
        {
        }

        public MembershipDataManager(string bucketName, IBucketFactory bucketFactory, ILoggerFactory loggerFactory, ClusterOptions clusterOptions) : base(bucketName, bucketFactory, clusterOptions)
        {
            _logger = loggerFactory.CreateLogger<MembershipDataManager>();
            _clusterOptions = clusterOptions;
        }


        public async Task DeleteMembershipTableEntries(string clusterId)
        {
            var deleteQuery = new QueryRequest($"delete from {BucketName} where clusterId = \"{clusterId}\" and docSubType = \"{DocSubTypes.Membership}\"");
            deleteQuery.ScanConsistency(ScanConsistency.RequestPlus);
            deleteQuery.Metrics(false);
            IQueryResult<MembershipEntry> result = await Bucket.QueryAsync<MembershipEntry>(deleteQuery).ConfigureAwait(false);
            if (!result.Success)
            {
                _logger.LogError(new Exception($"Unable to delete old silo records from couchbase {deleteQuery}"),$"{JsonConvert.SerializeObject(result.Errors)}");
            }

            _logger.LogInformation($"Deleted {result.Rows.Count} old Silo definitions from database. ");
        }

        public async Task CleanupDefunctSiloEntries()
        {
            await CleanupDefunctSiloEntries(MaxAgeExpiredSilosMin);
        }

        internal async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            double totalMinutes = (DateTimeOffset.Now - beforeDate).TotalMinutes;
            await CleanupDefunctSiloEntries(totalMinutes);
        }

        private async Task CleanupDefunctSiloEntries(double totalMinutes)
        {
            var deleteQuery = new QueryRequest($"delete from {BucketName} where clusterId = \"{_clusterOptions.ClusterId}\" and status = {(int)SiloStatus.Dead} and docSubType = \"{DocSubTypes.Membership}\" and STR_TO_MILLIS(Date_add_str(Now_utc(), -{totalMinutes}, 'minute'))  > STR_TO_MILLIS(iAmAliveTime)");
            deleteQuery.ScanConsistency(ScanConsistency.RequestPlus);
            deleteQuery.Metrics(false);
            IQueryResult<MembershipEntry> result = await Bucket.QueryAsync<MembershipEntry>(deleteQuery).ConfigureAwait(false);
            if (!result.Success)
            {
                _logger.LogError(new CouchbaseQueryResponseException($"{GetType().Name}: Error removing expired silo records from Couchbase. ", result.Status, result.Errors),"");
            }
        }

        public static int MaxAgeExpiredSilosMin => 20;

        #region GatewayProvider

        public async Task<IList<Uri>> GetGateWays()
        {
            MembershipTableData tableData = await ReadAll();
            List<MembershipEntry> result = tableData.Members.Select(tableDataMember => tableDataMember.Item1).ToList();
            List<Uri> r = result.Where(x => x.Status == SiloStatus.Active && x.ProxyPort != 0).Select(x =>
                {
                    x.SiloAddress.Endpoint.Port = x.ProxyPort; 
                    Uri address =  x.SiloAddress.ToGatewayUri();
                    return address;
                })
                .ToList();
            return r;
        }

        #endregion


        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            try
            {
                
                CouchbaseSiloRegistration serializable = CouchbaseSiloRegistrationUtility.FromMembershipEntry(_clusterOptions.ClusterId, entry, "0");
                IOperationResult<CouchbaseSiloRegistration> result = await Bucket.UpsertAsync(entry.SiloAddress.ToParsableString(), serializable).ConfigureAwait(false);
                return result.Success;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public async Task<MembershipTableData> ReadAll()
        {
            //todo is this the right query?
            var readAllQuery = new QueryRequest($"select meta().id from {BucketName} where docSubType = \"{DocSubTypes.Membership}\" and clusterId = \"{_clusterOptions.ClusterId}\"");
            readAllQuery.ScanConsistency(ScanConsistency.RequestPlus);
            readAllQuery.Metrics(false);
            IQueryResult<JObject> ids = await Bucket.QueryAsync<JObject>(readAllQuery).ConfigureAwait(false);

            List<string> idStrings = ids.Rows.Select(x => x["id"].ToString()).ToList();
            IDocumentResult<CouchbaseSiloRegistration>[] actuals = await Bucket.GetDocumentsAsync<CouchbaseSiloRegistration>(idStrings); //has no async version with batch reads
            var entries = new List<Tuple<MembershipEntry, string>>();
            foreach (IDocumentResult<CouchbaseSiloRegistration> actualRow in actuals)
            {
                if (actualRow?.Content == null || actualRow?.Document == null)
                {
                    _logger.LogWarning("Unable to get membership entry with query returned id. It is highly likely the Silo no longer exists. ");
                    continue;
                }
                
                entries.Add(CouchbaseSiloRegistrationUtility.ToMembershipEntry(actualRow.Content, actualRow.Document.Cas.ToString()));
            }

            return new MembershipTableData(entries, new TableVersion(0, "0"));
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            var entries = new List<Tuple<MembershipEntry, string>>();
            IOperationResult<CouchbaseSiloRegistration> row = await Bucket.GetAsync<CouchbaseSiloRegistration>(key.ToParsableString()).ConfigureAwait(false);
            if (row.Success)
            {
                entries.Add(CouchbaseSiloRegistrationUtility.ToMembershipEntry(row.Value, row.Cas.ToString()));
            }

            return new MembershipTableData(entries, new TableVersion(0, "0"));
        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            if (entry?.SiloAddress == null)
            {
                _logger.LogWarning($@"UpdateIAmAlive was handed null entry: {entry?.SiloAddress} status:{entry?.Status} start:{entry?.StartTime} lastAlive:{entry?.IAmAliveTime}");
                return;
            }
            
            IOperationResult<CouchbaseSiloRegistration> operationResult = await Bucket.GetAsync<CouchbaseSiloRegistration>(entry.SiloAddress.ToParsableString());
            
            if (!operationResult.Success || operationResult.Exception != null)
            {
                _logger.LogError($@"Error during get record in UpdateIAmAlive.  Database communication issue?  Will retry during next interval.  Error message: {operationResult.Exception?.Message}");
                return;
            }

            if (operationResult.Value == null)
            {
                //todo maybe we should just blow up and die here, but it does not seem like a safe thing to do this late in the release cycle.
//                throw new SiloUnavailableException("Record of my existence was removed from the database.  I should not exist.  Goodbye. ");
                _logger.LogError("Record of my existence (orleans silo) was removed from the database.  I should not exist. ");
                return;
            }

            operationResult.Value.IAmAliveTime = entry.IAmAliveTime;
            SiloAddress address = CouchbaseSiloRegistrationUtility.ToMembershipEntry(operationResult.Value).Item1.SiloAddress;
            await Bucket.UpsertAsync(address.ToParsableString(), operationResult.Value).ConfigureAwait(false);
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, TableVersion tableVersion, string eTag)
        {
            try
            {
                CouchbaseSiloRegistration serializableData = CouchbaseSiloRegistrationUtility.FromMembershipEntry(_clusterOptions.ClusterId, entry, eTag);
                IOperationResult<CouchbaseSiloRegistration> result = await Bucket.UpsertAsync(entry.SiloAddress.ToParsableString(), serializableData, ulong.Parse(eTag)).ConfigureAwait(false);
                return result.Success;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}