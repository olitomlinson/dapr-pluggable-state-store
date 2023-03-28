// ------------------------------------------------------------------------
// Copyright 2022 The Dapr Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ------------------------------------------------------------------------

using Google.Protobuf;
using Grpc.Core;
using Helpers;
using Npgsql;
using Dapr.PluggableComponents.Components;
using Dapr.PluggableComponents.Components.StateStore;
using Google.Rpc;

namespace DaprComponents.Services;

public class StateStoreService : IStateStore, IPluggableComponentFeatures, IPluggableComponentLiveness, ITransactionalStateStore
{
    private readonly ILogger<StateStoreService> _logger;
    private StateStoreInitHelper _stateStoreInitHelper;
    public StateStoreService(ILogger<StateStoreService> logger, StateStoreInitHelper stateStoreInitHelper)
    {
        _logger = logger;
        _stateStoreInitHelper = stateStoreInitHelper;
    }


    public async Task DeleteAsync(StateStoreDeleteRequest request, CancellationToken cancellationToken = default)
    {
         _logger.LogInformation($"{nameof(DeleteAsync)}");
        
        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            var tran = await conn.BeginTransactionAsync();
            try 
            {
                await dbfactory(request.Metadata).DeleteAsync(request.Key, request.ETag ?? String.Empty, tran);
            }
            catch(Exception ex)
            {   
                await tran.RollbackAsync();
                _logger.LogDebug($"{nameof(DeleteAsync)} - rolled back transaction");
                throw;
            }
            await tran.CommitAsync();
            _logger.LogDebug($"{nameof(DeleteAsync)} - transaction commited");
        }
        return;
    }

    private static async Task ThrowGrpcException(string message)
    {
        var badRequest = new BadRequest();
        var des = message;
        badRequest.FieldViolations.Add(    
        new Google.Rpc.BadRequest.Types.FieldViolation
            {        
                Field = message,
                Description = des
            });

        var baseStatusCode = Grpc.Core.StatusCode.FailedPrecondition;
        var status = new Google.Rpc.Status{    
        Code = (int)baseStatusCode
        };

        status.Details.Add(Google.Protobuf.WellKnownTypes.Any.Pack(badRequest));

        var metadata = new Metadata();
        metadata.Add("grpc-status-details-bin", status.ToByteArray());
        throw new RpcException(new Grpc.Core.Status(baseStatusCode, message), metadata);
    }

    public async Task<StateStoreGetResponse?> GetAsync(StateStoreGetRequest request, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"{nameof(GetAsync)}");

        string value = "";
        string etag = "";

        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            bool notFound = false;

            try 
            {
                (value, etag) = await dbfactory(request.Metadata).GetAsync(request.Key);              
                if (value == null)
                    notFound = true;
            } 
            catch(PostgresException ex) when (ex.TableDoesNotExist())
            {
                notFound = true;
            }
            catch(StateStoreInitHelperException ex) when (ex.Message.StartsWith("Missing Tenant Id"))
            {
                await ThrowGrpcException(ex.Message);
            }

            if (notFound)
            {
                _logger.LogDebug($"{nameof(GetAsync)} - State not found with key : [{request.Key}]");
                return new StateStoreGetResponse();
            } 
        }

        return new StateStoreGetResponse
        {
            Data = System.Text.Encoding.UTF8.GetBytes(value),
            ETag = etag
        };  
    }

    public async Task<string[]> GetFeaturesAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"{nameof(GetFeaturesAsync)}");

        string[] response = { "ETAG", "TRANSACTIONAL" };
        return response;
    }

    public async Task InitAsync(MetadataRequest request, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"{nameof(InitAsync)}");

        await _stateStoreInitHelper.InitAsync(request.Properties);

        return;
    }

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"{nameof(PingAsync)}");
        return;
    }

    public async Task SetAsync(StateStoreSetRequest request, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"{nameof(SetAsync)}");
                
        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            NpgsqlTransaction tran = null;
            try
            {
                // TODO : Need to implement 'something' here with regards to 'isBinary',
                // but I do not know what this is trying to achieve. See existing pgSQL built-in component 
                // https://github.com/dapr/components-contrib/blob/d3662118105a1d8926f0d7b598c8b19cd9dc1ccf/state/postgresql/postgresdbaccess.go#L135
                
                tran = await conn.BeginTransactionAsync();
                var value = System.Text.Encoding.UTF8.GetString(request.Value.Span);
                await dbfactory(request.Metadata).UpsertAsync(request.Key, value, request.ETag ?? String.Empty, tran);   
                await tran.CommitAsync();
            }
            catch(Exception ex)
            {
                await tran.RollbackAsync();
                _logger.LogError(ex, $"{nameof(SetAsync)} - Rollback");
                throw;
            }
        }   
        return;
    }

    public async Task TransactAsync(StateStoreTransactRequest request, CancellationToken cancellationToken = default)
    {

        _logger.LogInformation($"{nameof(TransactAsync)} - Set/Delete");

        if (!request.Operations.Any())
            return;

        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            var tran = await conn.BeginTransactionAsync();
            try 
            {
                foreach(var op in request.Operations)
                {
                    await op.Visit(
                        onDeleteRequest: async (x) => {
                            var db = dbfactory(x.Metadata);
                            await db.DeleteAsync(x.Key, x.ETag ?? String.Empty, tran);
                        },
                        onSetRequest: async (x) => {     
                            var db = dbfactory(x.Metadata);
                            // TODO : Need to implement 'something' here with regards to 'isBinary',
                            // but I do not know what this is trying to achieve. See existing pgSQL built-in component 
                            // https://github.com/dapr/components-contrib/blob/d3662118105a1d8926f0d7b598c8b19cd9dc1ccf/state/postgresql/postgresdbaccess.go#L135
                            var body = x.Value.ToArray();
                            var payload = System.Text.Encoding.UTF8.GetString(body);
                            await db.UpsertAsync(x.Key, payload, x.ETag ?? String.Empty, tran); 
                        }
                    );
                }
                await tran.CommitAsync();
            }
            catch(Exception ex)
            {
                await tran.RollbackAsync();
                _logger.LogError(ex, $"{nameof(TransactAsync)} - Rollback");
                throw;
            } 
        }
    }
}