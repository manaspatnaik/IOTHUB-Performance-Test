using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeviceSimulator
{
    public class Program
    {
        private const string IotHubUri = "<IOTHUB URI>";
        private const string IotHubconnectionString = "<IOT HUB Connection String>";


     


        private static async Task ReceiveC2dAsync(DeviceClient client)
        {
          
            while (true)
            {
                Microsoft.Azure.Devices.Client.Message receivedMessage = await client.ReceiveAsync();
                if (receivedMessage == null) continue;

                Console.ForegroundColor = ConsoleColor.Yellow;
                string msgBody = String.Empty; ;
                using (var reader = new StreamReader(receivedMessage.BodyStream, Encoding.ASCII))
                {
                    msgBody = reader.ReadToEnd();
                  
                }
                Console.WriteLine($"Received Message ID: {receivedMessage.MessageId} , " +
                    $"Body ::  {msgBody}");
                Console.ResetColor();

                await client.CompleteAsync(receivedMessage);
            }
        }

        static string DeviceConnectionString(string ioTHubHostName, Device device)
        {
            return string.Format("HostName={0};CredentialScope=Device;DeviceId={1};SharedAccessKey={2}",
                ioTHubHostName,
                device.Id,
                device.Authentication.SymmetricKey.PrimaryKey);
        }

        static async Task<IEnumerable<Device>> GetDevices(string iothubConString,int maxdevices=500)
        {
            var registryManager = RegistryManager.CreateFromConnectionString(iothubConString);

           return await registryManager.GetDevicesAsync(maxdevices); // Time 1 sek

        }
        static async void SetUpListener()
        {
           
            var devices =  GetDevices(IotHubconnectionString).Result.OrderBy(p=>p.Id);
            
            int index = 0;
            Console.WriteLine($"\nSetting Up Listeners.");
            foreach (var item in devices)
            {
                var connectionString = DeviceConnectionString(IotHubUri, item);
                var client = DeviceClient.CreateFromConnectionString(connectionString, Microsoft.Azure.Devices.Client.TransportType.Amqp);

                try
                {
                    ReceiveC2dAsync(client);
                    Console.WriteLine($"{item.Id} Listening .. ");
                }
                catch(Exception exp)
                {
                    Console.WriteLine($"Exceptiong !!! {item.Id} . setting up listener failed .. ");
                }
            }
            Console.WriteLine($"\nListeners setup complete. .. ");


        }
        private static void Main(string[] args)
        {
            Console.WriteLine("Start Simulated device\n");
            SetUpListener();
        
            Console.ReadLine();
        }
    }
}
