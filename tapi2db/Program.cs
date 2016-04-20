using System;
using System.Configuration;
using System.Globalization;
using System.Threading;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.ComponentModel;
using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using System.Collections.Specialized;
using JulMar.Atapi;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace tapi2db
{
    /*
     * Description: Программный продукт для записи и последующей обработки событий на 
     *              мини-АТС ipLDK 300. Программа подключается к АТС по стандартному интерфейсу
     *              TAPI (Telephony Application Programming Interface) поверх tcp/ip.
     *              Конфигурация подключения по интерфейсу TAPI указывается при установке TSP драйвера.
     * Date:        14.04.2016
     * Author:      Lev ICHI Zaplatin, dev@ichi.su
     * Version:     0.001
     * 
     */
    class Program
    {
        static NameValueCollection config;
        static MemcachedClientConfiguration memconfig;
        static MemcachedClient memclient;
        static MongoClient mongoclient;
        static IMongoDatabase mongodb;
        static StreamWriter log;

        static void Main(string[] args)
        {
            Console.WriteLine("So...");

            // Подключаем локальный конфиг
            config = config = ConfigurationManager.AppSettings;

            // Готовим конфиг для Memcached
            memconfig = new MemcachedClientConfiguration();
            // Протокол. В этом месте есть баг! Флаг выставляется НЕправильно!!!
            memconfig.Protocol = MemcachedProtocol.Text;
            memconfig.AddServer(string.Format("{0}:{1}", config.Get("memcached_server"), config.Get("memcached_port")));
            memconfig.SocketPool.ReceiveTimeout = new TimeSpan(0, 0, 10);
            memconfig.SocketPool.ConnectionTimeout = new TimeSpan(0, 0, 10);
            memconfig.SocketPool.DeadTimeout = new TimeSpan(0, 0, 20);

            try
            {
                // Если нужно подключаем Log
                if (config.Get("log_use") == "1" || config.Get("log_use") == "yes")
                {
                    log = File.AppendText(string.Format("{0}{1}", config.Get("work_folder"), config.Get("log_file")));
                    Console.WriteLine("using\t\tLog: yes");
                }
                else
                {
                    Console.WriteLine("using\t\tLog: no");
                }

                // Если нужно подключаем Memcached
                if (config.Get("memcached_use") == "1" || config.Get("memcached_use") == "yes")
                {
                    memclient = new MemcachedClient(memconfig);
                    Console.WriteLine("using\t\t\tMemcached: yes");
                    Console.WriteLine(string.Format("\t\tMemcached: {0}:{1}", config.Get("memcached_server"), config.Get("memcached_port")));
                }
                else
                {
                    Console.WriteLine("using\t\tMemcached: no");
                }
                // Если нужно подключаем MongoDB
                if (config.Get("mongodb_use") == "1" || config.Get("mongodb_use") == "yes")
                {
                    Console.WriteLine("using\t\tMongoDB: yes");
                    Console.WriteLine(string.Format("\t\tMongoDB: {0}:{1}", config.Get("mongodb_server"), config.Get("mongodb_port")));
                    connect_mongodb();
                }
                else
                {
                    Console.WriteLine("using\t\tMongoDB: no");
                }

            }
            catch (MemcachedException e)
            {
                Console.WriteLine(string.Format("\t\tMemcached: {0}!", e.ToString()));
            }
            catch (MongoException e)
            {
                if (e.Message != "Command create failed: collection already exists.")
                {
                    Console.WriteLine(string.Format("\t\tMongoDB: {0}!", e.ToString()));
                }
            }
            catch (FileLoadException e)
            {
                Console.WriteLine(string.Format("\t\tLog: {0}!", e.ToString()));
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine(string.Format("\t\tLog: {0}!", e.ToString()));
            }


            JulMar.Atapi.TapiManager tapiManager = new JulMar.Atapi.TapiManager("tapi2db");
            if (!tapiManager.Initialize())
            {
                Console.WriteLine("TapiManager\tfailed to initialize and it's very-very bad...");
                Console.WriteLine("Press Enter to exit... :(");
                Console.ReadLine();
            }
            else
            {
                Console.WriteLine("TapiManager\tinitialize!");

                foreach (TapiLine line in tapiManager.Lines)
                {
                    if (line != null)
                    {
                        try
                        {
                            line.NewCall += new EventHandler<NewCallEventArgs>(line_NewCall);
                            line.CallInfoChanged += new EventHandler<CallInfoChangeEventArgs>(line_CallInfoChanged);
                            line.CallStateChanged += new EventHandler<CallStateEventArgs>(line_CallStateChanged);
                            line.Monitor();
                        }
                        catch (TapiException ex)
                        {
                            if (ex.Message.Substring(0, 36) != "lineOpen failed [0xFFFFFFFF8000004B]")
                            {
                                Console.WriteLine(ex.Message);
                            }
                        }
                    }
                }
                Console.WriteLine("________________________________________________________________________________");
                Console.WriteLine("    __       _______ .___________.__     _______.     _______   ______    __ ");
                Console.WriteLine("   |  |     |   ____||           (_ )   /       |    /  _____| /  __  \\  |  |");
                Console.WriteLine("   |  |     |  |__   `---|  |----`|/   |   (----`   |  |  __  |  |  |  | |  |");
                Console.WriteLine("   |  |     |   __|      |  |           \\   \\       |  | |_ | |  |  |  | |  |");
                Console.WriteLine("   |  `----.|  |____     |  |       .----)   |      |  |__| | |  `--'  | |__|");
                Console.WriteLine("   |_______||_______|    |__|       |_______/        \\______|  \\______/  (__)");
                Console.WriteLine("________________________________________________________________________________");
            }

        }

        static void connect_mongodb()
        {
            int mongodb_reconnect = 0;
            do
            {
                mongoclient = new MongoClient(string.Format("mongodb://{0}:{1}", config.Get("mongodb_server"), config.Get("mongodb_port")));
                mongodb = mongoclient.GetDatabase("pbx_event");
                // Создаем нужные коллекция, они конечно и сами могут создаться при записи, но к сожалению без параметра Capped, позволяющего
                // отслеживать изменения в коллекциях
                mongodb.CreateCollection("line_all", new CreateCollectionOptions { Capped = true, MaxSize = 1024 * 1024 * 1024 });
                mongodb.GetCollection<BsonDocument>("line_all").Indexes.CreateOneAsync(
                    Builders<BsonDocument>.IndexKeys.Ascending("expire"),
                    new CreateIndexOptions { ExpireAfter = TimeSpan.FromHours(1) }
                );
                if (mongoclient.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Disconnected )
                {
                    Console.WriteLine(string.Format("MongoDB\t\tConnect - fail #{0}",mongodb_reconnect++));
                    Thread.Sleep(1);
                }
            } while (mongoclient.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Disconnected);
        }

        static void line_NewCall(object sender, NewCallEventArgs e)
        {
            BsonDocument document = make_document(e);
            save_info(document);
        }

        static void line_CallStateChanged(object sender, CallStateEventArgs e)
        {
            BsonDocument document = make_document(e);
            save_info(document);
        }

        static void line_CallInfoChanged(object sender, CallInfoChangeEventArgs e)
        {
            BsonDocument document = make_document(e);
            save_info(document);
        }

        static BsonDocument make_document(NewCallEventArgs e)
        {
            BsonDocument document = new BsonDocument { };
            document.Add("call_action", "call_new");
            document = make_document(document, e);
            return document;
        }

        static BsonDocument make_document(CallStateEventArgs e)
        {
            BsonDocument document = new BsonDocument { };
            document.Add("call_action", "change_state");
            document.Add("call_stateOld", e.OldCallState.ToString());
            document = make_document(document, e);
            return document;
        }

        static BsonDocument make_document(CallInfoChangeEventArgs e)
        {
            BsonDocument document = new BsonDocument { };
            document.Add("call_action", "change_info");
            document.Add("info_change", e.Change.ToString());
            document = make_document(document, e);
            return document;
        }

        static BsonDocument make_document(BsonDocument document, dynamic e = null)
        {

            // State
            document.Add("call_state", e.Call.CallState.ToString());
            // Caller
            document.Add("call_callerId", e.Call.CallerId);
            document.Add("call_callerName", e.Call.CallerName);
            // Called
            document.Add("call_calledId", e.Call.CalledId.Trim());
            document.Add("call_calledName", e.Call.CalledName.Trim());
            // Connected
            document.Add("call_connectedId", e.Call.ConnectedId.Trim());
            document.Add("call_connectedName", e.Call.ConnectedName.Trim());
            // redirectioing
            document.Add("call_redirectingId", e.Call.RedirectingId.Trim());
            document.Add("call_redirectingName", e.Call.RedirectingName.Trim());
            // Redirection
            document.Add("call_redirectionId", e.Call.RedirectionId.Trim());
            document.Add("call_redirectionName", e.Call.RedirectionName.Trim());
            //  
            document.Add("call_origin", e.Call.CallOrigin.ToString());
            document.Add("call_reason", e.Call.CallReason.ToString());
            document.Add("call_trunkId", e.Call.TrunkId);
            document.Add("call_bearerMode", e.Call.BearerMode.ToString());
            document.Add("call_name", e.Call.CallerName);
            // Line
            document.Add("call_lineId", e.Call.Line.Id);
            document.Add("call_permanentId", e.Call.Line.PermanentId);
            document.Add("call_totalCallCount", e.Call.Line.TotalCallCount);
            document.Add("call_deviceSpecificExtensionID", e.Call.Line.DeviceSpecificExtensionID);
            //
            /**/
            document.Add("call_privilege", e.Call.Privilege.ToString());
            document.Add("call_dateRate", e.Call.DataRate);
            document.Add("call_dialSpeed", e.Call.DialSpeed);
            document.Add("call_dialPause", e.Call.DialPause);
            document.Add("call_mediaDetection", e.Call.MediaDetection ? 1 : 0);
            document.Add("call_digitDuration", e.Call.DigitDuration);
            document.Add("call_waitForDialtoneDuration", e.Call.WaitForDialtoneDuration);
            document.Add("call_appSpecificData", e.Call.AppSpecificData);
            //{ "call_callData", e.Call.CallData.ToString() );
            document.Add("call_lastEventTime", e.Call.LastEventTime.Ticks);
            document.Add("call_mediaMode", e.Call.MediaMode.ToString());
            document.Add("call_relatedId", e.Call.RelatedId);
            /**/
            // Features
            document.Add("call_features_canAccept", e.Call.Features.CanAccept ? 1 : 0);
            document.Add("call_features_canAddToConference", e.Call.Features.CanAddToConference ? 1 : 0);
            document.Add("call_features_canAnswer", e.Call.Features.CanAnswer ? 1 : 0);
            document.Add("call_features_canBlindTransfer", e.Call.Features.CanBlindTransfer ? 1 : 0);
            document.Add("call_features_canCompleteCall", e.Call.Features.CanCompleteCall ? 1 : 0);
            document.Add("call_features_canCompleteTransfer", e.Call.Features.CanCompleteTransfer ? 1 : 0);
            document.Add("call_features_canDial", e.Call.Features.CanDial ? 1 : 0);
            document.Add("call_features_canDrop", e.Call.Features.CanDrop ? 1 : 0);
            document.Add("call_features_canGatherDigits", e.Call.Features.CanGatherDigits ? 1 : 0);
            document.Add("call_features_canGenerateDigits", e.Call.Features.CanGenerateDigits ? 1 : 0);
            document.Add("call_features_canGenerateTone", e.Call.Features.CanGenerateTone ? 1 : 0);
            document.Add("call_features_canHold", e.Call.Features.CanHold ? 1 : 0);
            document.Add("call_features_canMonitorDigits", e.Call.Features.CanMonitorDigits ? 1 : 0);
            document.Add("call_features_canMonitorMedia", e.Call.Features.CanMonitorMedia ? 1 : 0);
            document.Add("call_features_canMonitorTones", e.Call.Features.CanMonitorTones ? 1 : 0);
            document.Add("call_features_canPark", e.Call.Features.CanPark ? 1 : 0);
            document.Add("call_features_canPrepareAddToConference", e.Call.Features.CanPrepareAddToConference ? 1 : 0);
            document.Add("call_features_canRedirect", e.Call.Features.CanRedirect ? 1 : 0);
            document.Add("call_features_canRemoveFromConference", e.Call.Features.CanRemoveFromConference ? 1 : 0);
            document.Add("call_features_canSecureCall", e.Call.Features.CanSecureCall ? 1 : 0);
            document.Add("call_features_canSendUserUserInfo", e.Call.Features.CanSendUserUserInfo ? 1 : 0);
            document.Add("call_features_canSetCallParams", e.Call.Features.CanSetCallParams ? 1 : 0);
            document.Add("call_features_canSetMediaControl", e.Call.Features.CanSetMediaControl ? 1 : 0);
            document.Add("call_features_canSetTerminal", e.Call.Features.CanSetTerminal ? 1 : 0);
            document.Add("call_features_canSetupConference", e.Call.Features.CanSetupConference ? 1 : 0);
            document.Add("call_features_canSetupTransfer", e.Call.Features.CanSetupTransfer ? 1 : 0);
            document.Add("call_features_canSwapHold", e.Call.Features.CanSwapHold ? 1 : 0);
            document.Add("call_features_canUnhold", e.Call.Features.CanUnhold ? 1 : 0);
            document.Add("call_features_canReleaseUserUserInfo", e.Call.Features.CanReleaseUserUserInfo ? 1 : 0);
            document.Add("call_features_canSetTreatment", e.Call.Features.CanSetTreatment ? 1 : 0);
            document.Add("call_features_canSetQos", e.Call.Features.CanSetQos ? 1 : 0);
            document.Add("call_features_canSetCallData", e.Call.Features.CanSetCallData ? 1 : 0);
            //
            document.Add("call_phone", Convert.ToInt64(e.Call.Address.Address));
            document.Add("line_name", e.Call.Line.Name);
            document.Add("event_unixtime", UnixMiliTimeNow());
            document.Add("event_datetime", DateTime.Now.ToString());

            return document;
        }

        static void save_info(BsonDocument document)
        {
            string my_event = string.Format("call_action:call_new;call_state:{0};caller_id:{1};call_origin:{2};call_reason:{3};call_trunkid:{4};call_bearermode:{5};call_name:{6};call_lineid:{7};call_phone:{8};event_time:{9}",
                document.GetValue("call_action").ToString(),
                document.GetValue("call_state").ToString(),
                document.GetValue("call_origin").ToString(),
                document.GetValue("call_reason").ToString(),
                document.GetValue("call_trunkId").ToString(),
                document.GetValue("call_bearerMode").ToString(),
                document.GetValue("call_name").ToString(),
                document.GetValue("call_lineId").ToString(),
                document.GetValue("call_phone").ToString(),
                UnixMiliTimeNow());

            DateTime datetime_begin = DateTime.Now;
            if (config.Get("memcached_use") == "1" || config.Get("memcached_use") == "yes")
            {
                save_info_memcached(document.GetElement("call_phone").ToString(), my_event);
            }
            if (config.Get("mongodb_use") == "1" || config.Get("mongodb_use") == "yes")
            {
                save_info_mongodb(document);
            }
            if (config.Get("log_use") == "1" || config.Get("log_use") == "yes")
            {
                save_info_log(my_event);
            }
            DateTime datetime_end = DateTime.Now;
            var culture = new CultureInfo("ru-RU");
            string datetime_diff = datetime_end.Subtract(datetime_begin).ToString();
            Console.WriteLine(string.Format("{0}: {1}: {2}\t{3}{4}  {5}", datetime_begin.ToString(culture), datetime_diff, document.GetValue("call_action").ToString(), (document.GetValue("call_action").ToString() == "call_new" ? "\t" : ""), document.GetValue("call_phone").ToString(), document.GetValue("call_callerId").ToString()));
        }

        static void save_info_memcached(string line, string my_event)
        {
            int idx = Convert.ToInt32(memclient.Get<string>(string.Format("line_{0}_idx", line)));

            Console.WriteLine(string.Format("line_{0}_idx: {1}", line, idx.ToString()));
            idx = idx + 1;

            memclient.Store(StoreMode.Set, string.Format("line_{0}_idx", line), idx.ToString(), new TimeSpan(24, 0, 0));
            memclient.Store(StoreMode.Set, string.Format("line_{0}_{1}", line, idx.ToString()), my_event, new TimeSpan(24, 0, 0));
        }

        static void save_info_mongodb(BsonDocument document)
        {
            if (mongoclient.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Disconnected)
            {
                connect_mongodb();
            }
            mongodb.GetCollection<BsonDocument>("line_all").InsertOneAsync(document);
        }

        static void save_info_log(string my_event)
        {
            log.WriteLine(my_event);
        }

        static string UnixMiliTimeNow()
        {
            double militime = DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds;
            return militime.ToString();
        }

    }
}
