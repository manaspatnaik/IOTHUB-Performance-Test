using Microsoft.Azure.Devices;
using SimpleLogger;
using SimpleLogger.Logging.Handlers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace C2DServiceSimulator
{
    class Program
    {
        private const string IotHubHostName = "<IOTHUB HOSTNAME>";
        private const string IotHubconnectionString = "<IOTHUB Owner Connection String>>";
        static ServiceClient _serviceClient;

        static ConcurrentDictionary<string, C2DMetrics> _perDeviceC2DStatistics;

        const int virtualDevicesCnt = 10;
     

        static void Main(string[] args)
        {
            // Adding handler - to show log messages (ILoggerHandler)
            Logger.LoggerHandlerManager
                .AddHandler(new ConsoleLoggerHandler())
                .AddHandler(new FileLoggerHandler())
                .AddHandler(new DebugConsoleLoggerHandler());

            Logger.Log("Start Test");
            Console.WriteLine("Start Test !!!");
          
            _serviceClient = ServiceClient.CreateFromConnectionString(IotHubconnectionString, TransportType.Amqp);
           
            var devices =  GetDevices().ToList().OrderBy(p=> p.Id);
            
            _perDeviceC2DStatistics = new ConcurrentDictionary<string, C2DMetrics>();
            
            //int indexer = 0;
            //foreach (var item in devices)
            //{
            //    Console.WriteLine($"Device ID :: {item.Id} ");
            //    Console.WriteLine($"Device Count :: {devices.Count()} ");

            //}
            Console.WriteLine($"Device Count :: {devices.Count()} ");
            Logger.Log($"Devices Retrived from registry : {devices.Count()} ");

            Logger.Log($"Initializing C2D Messages.");
            var tasks = devices.Select(dev => StartC2DStress(2, dev)).ToList();
            var wrapperTask = Task.WhenAll(tasks);

            do
            {

                Console.WriteLine($"Task Status , Total::{tasks.Count}  , " +
                    $"Incomplete :: {tasks.Count(t => !t.IsCompleted)} , " +
                    $"Completed :: {tasks.Count(t => t.IsCompleted)}");
            }
            while (!wrapperTask.Wait(TimeSpan.FromSeconds(3)));
            //Task.WaitAll(tasks);
            foreach (var item in _perDeviceC2DStatistics)
            {
                var metric = item.Value;
                var msg = $"Device Metric :: {item.Key} , " +
                          $"Sent- {metric._msgsSentCounter}, " +
                          $"Success- {metric._msgsSuccessCounter}, " +
                          $"Failure- {metric._msgsFailureCounter}";

                Logger.Log(msg);
               
                Console.WriteLine($"Device Metric :: {item.Key} , " +
                                  $"Sent- {metric._msgsSentCounter}, " +
                                  $"Success- {metric._msgsSuccessCounter}, " +
                                  $"Failure- {metric._msgsFailureCounter}   ");
                
            }
            Logger.Log("End Test");
          
            Console.WriteLine("Complete!!!");
          Console.ReadKey();
        }
        static string DeviceConnectionString(string ioTHubHostName, Device device)
        {
            return string.Format("HostName={0};CredentialScope=Device;DeviceId={1};SharedAccessKey={2}",
                ioTHubHostName,
                device.Id,
                device.Authentication.SymmetricKey.PrimaryKey);
        }

        static IEnumerable<Device> GetDevices(int maxdevices = 500)
        {
            var registryManager = RegistryManager.CreateFromConnectionString(IotHubconnectionString);

            var devices=  registryManager.GetDevicesAsync(maxdevices); 

            Task.WaitAll();

            return devices.Result;
        }
     
        static async Task<C2DMetrics> StartC2DStress(int msgload, Device device)
        {
            C2DMetrics metric = new C2DMetrics();
            Console.WriteLine($"Device ID :: {device.Id} ");
            metric.DeviceID = device.Id;
            for (int msgcnt = 0; msgcnt < msgload; msgcnt++)
            {
                metric._msgsSentCounter++;
                string msg = $"Message from Cloud >> Device ID {device.Id} :: MSG Count :: {msgcnt}";
                var message = new Message(Encoding.ASCII.GetBytes(msg));
                message.MessageId = new Guid().ToString();
                

                try
                {
                   await _serviceClient.SendAsync(device.Id, message);
                   metric._msgsSuccessCounter++;
                }
                catch (Exception exp)
                {
                   Console.WriteLine($"!!!Exception :: {device.Id} {exp.Message}");
                   //Logger.Log($"!!!Exception:: { device.Id}{ exp.Message}");
                   metric._msgsFailureCounter++;
                   await Task.Delay(10000);
                }
                finally
                {

                    if (!_perDeviceC2DStatistics.ContainsKey(device.Id))
                        _perDeviceC2DStatistics.TryAdd(device.Id, metric);
                    else
                        _perDeviceC2DStatistics[device.Id] = metric;


                }

            }
         
            return metric;
         
        }

    }


    struct C2DMetrics
        {
        public string DeviceID;
        public long _msgsSentCounter;
        public long _msgsSuccessCounter;
        public long _msgsFailureCounter;
        
    }
}
