using System;
using System.DirectoryServices.AccountManagement;
using System.IO;
using System.Net.Security;
using System.Security.Principal;
using System.ServiceModel;
using System.Threading.Tasks;
using NetTcpHub.Contracts;

namespace NetTcpHub.Client
{

    public class TcpHubClientOptions
    {
        public string ServerName { get; set; }
        public int Port { get; set; } = 55555;
        public string ServicePath { get; set; } = "NetTcpHub";
        public bool AllowNtlm { get; set; } = true;
        public long MaxReceivedMessageSize { get; set; } = 1024 * 1024;
        public int OpenTimeoutSeconds { get; set; } = 30;
        public int SendTimeoutSeconds { get; set; } = 60;
        public int ReceiveTimeoutMinutes { get; set; } = 10;
        public int CloseTimeoutSeconds { get; set; } = 30;

        public bool AutoReconnect { get; set; } = true;
    }

    public class MessageReceivedEventArgs : EventArgs
    {
        public string UserName { get; set; }
        public string SessionId { get; set; }
        public HubMessage Message { get; set; }
    }

    public class FileReceivedEventArgs : EventArgs
    {
        public string SessionId { get; set; }
        public string FilePath { get; set; }
    }

    public class ConnectionChangedEventArgs : EventArgs
    {
        public string SessionId { get; set; }
        public ConnectionChangedEventArgs(ConnectionStatus connectionStatus) => Status = connectionStatus;
        public ConnectionStatus Status { get; }
    }

    public delegate void EventHandlerExt<TSender, TEventArgs>(TSender sender, TEventArgs e);
    public class TcpHubClient : IDisposable
    {
        private class TcpHubClientMethods : IClientMethods
        {
            private readonly TcpHubClient tcpHubClient;
            public TcpHubClientMethods(TcpHubClient tcpHubClient) => this.tcpHubClient = tcpHubClient;
            public void Send(HubMessage message) => tcpHubClient.ReceivedMessage(message);
            public void Upload(Stream stream) => tcpHubClient.ReceivedStream(stream);
        }

        private DuplexChannelFactory<IServerMethods> channelFactory;
        private IServerMethods proxy;
        private TcpHubClientMethods clientMethods;
        private IClientChannel clientChannel;
        private readonly bool reconnectEnabled;
        private readonly NetTcpBinding binding;

        private readonly object sync = new object();

        public bool IsConnected => Status == ConnectionStatus.Connected;
        public ConnectionStatus Status => clientChannel is null ? ConnectionStatus.NotConnected : (ConnectionStatus)clientChannel.State;
        //public string Username => ConnectionInfo?.UserIdentity;


        private readonly EndpointAddress address;
        private readonly bool allowNtlm;
        public TcpHubClient(TcpHubClientOptions options)
        {
            if (options is null) throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.ServerName)) throw new ArgumentException("Server name is required.");

            binding = new NetTcpBinding(SecurityMode.Transport);
            binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
            binding.Security.Transport.ProtectionLevel = ProtectionLevel.EncryptAndSign;
            binding.MaxReceivedMessageSize = options.MaxReceivedMessageSize;
            binding.OpenTimeout = TimeSpan.FromSeconds(options.OpenTimeoutSeconds);
            binding.SendTimeout = TimeSpan.FromSeconds(options.SendTimeoutSeconds);
            binding.ReceiveTimeout = TimeSpan.FromMinutes(options.ReceiveTimeoutMinutes);
            binding.CloseTimeout = TimeSpan.FromSeconds(options.CloseTimeoutSeconds);
            reconnectEnabled = options.AutoReconnect;

            //binding.ReliableSession.Enabled = true;
            //binding.ReliableSession.InactivityTimeout = TimeSpan.FromMinutes(5);

            var serverUpn = UserPrincipal.Current.UserPrincipalName;
            address = new EndpointAddress(new Uri($"net.tcp://{options.ServerName}:{options.Port}/{options.ServicePath.Trim('/')}"), EndpointIdentity.CreateUpnIdentity(serverUpn));
            allowNtlm = options.AllowNtlm;
        }


        public void Connect()
        {
            if (disposed) return;

            lock (sync)
            {
                if (IsConnected) return;

                var context = new InstanceContext(new TcpHubClientMethods(this));
                channelFactory = new DuplexChannelFactory<IServerMethods>(context, binding, address);
                if (!allowNtlm) channelFactory.Credentials.Windows.AllowNtlm = false;
                channelFactory.Credentials.Windows.AllowedImpersonationLevel = TokenImpersonationLevel.Identification;

                try
                {
                    proxy = channelFactory.CreateChannel();
                    clientChannel = (IClientChannel)proxy;

                    clientChannel.Opening += Channel_Opening;
                    clientChannel.Opened += Channel_Opened;
                    clientChannel.Faulted += Channel_Faulted;
                    clientChannel.Closing += Channel_Closing;
                    clientChannel.Closed += Channel_Closed;

                    clientChannel.Open();
                }
                catch
                {
                    Cleanup();
                    throw;
                }
            }
        }
        public void Disconnect()
        {
            Cleanup();
        }
        private void Channel_Opening(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Connecting);
        private void Channel_Opened(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Connected);
        private void Channel_Faulted(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Faulted);
        private void Channel_Closing(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Disconnecting);
        private void Channel_Closed(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Disconnected);

        public event EventHandlerExt<TcpHubClient, MessageReceivedEventArgs> MessageReceived;
        public event EventHandlerExt<TcpHubClient, FileReceivedEventArgs> FileReceived;
        public event EventHandlerExt<TcpHubClient, ConnectionChangedEventArgs> ConnectionChanged;

        public void SendMessage(HubMessage message)
        {
            EnsureConnected();
            proxy.Send(message);
        }

        public void SendFile(string filePath)
        {
            EnsureConnected();
            using (FileStream fs = File.OpenRead(filePath))
            {
                proxy.Upload(fs);
            }
        }


        private void ReceivedMessage(HubMessage message)
        {
            OnMessageReceived(message);
        }

        private void ReceivedStream(Stream stream)
        {
            string tempFile = Path.GetTempFileName();
            using (var fileStream = new FileStream(tempFile, FileMode.Open, FileAccess.ReadWrite))
            {
                stream.CopyTo(fileStream);
            }

            OnFileReceived(tempFile);
        }

        public void UploadFile(string FilePath)
        {
            using (FileStream fileStream = File.OpenRead(FilePath))
            {
                proxy.Upload(fileStream);
            }
        }

        private void EnsureConnected()
        {
            if (!IsConnected) throw new InvalidOperationException("Client is not connected.");
        }

        private void OnMessageReceived(HubMessage message) => MessageReceived?.Invoke(this, new MessageReceivedEventArgs { Message = message });
        private void OnFileReceived(string filePath) => FileReceived?.Invoke(this, new FileReceivedEventArgs { FilePath = filePath });
        private void OnConnectionChanged(ConnectionStatus connectionStatus) => ConnectionChanged?.Invoke(this, new ConnectionChangedEventArgs(connectionStatus));

        private int reconnectAttempts;
        private bool reconnecting;
        private async Task AttemptReconnect()
        {
            if (!reconnectEnabled) return;

            if (reconnecting) return;
            try
            {
                reconnecting = true;
                //status = ConnectionStatus.Reconnecting;

                while (!disposed && !IsConnected)
                {
                    try
                    {
                        Cleanup();

                        Connect();

                        reconnectAttempts = 0;

                        return;
                    }
                    catch
                    {
                        reconnectAttempts++;
                        var delaySeconds = Math.Min((int)Math.Pow(2, reconnectAttempts), 60);
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                    }
                }
            }
            finally
            {
                reconnecting = false;
            }
        }

        private void Cleanup()
        {
            try
            {
                if (clientChannel != null)
                {
                    if (clientChannel.State == CommunicationState.Faulted) clientChannel.Abort();
                    else clientChannel.Close();
                }
            }
            catch { clientChannel?.Abort(); }

            try
            {
                if (channelFactory != null)
                {
                    if (channelFactory.State == CommunicationState.Faulted) channelFactory.Abort();
                    else channelFactory.Close();
                }
            }
            catch { channelFactory?.Abort(); }

            clientChannel = null;
            proxy = null;
            channelFactory = null;
        }

        private bool disposed;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing) Cleanup();
            disposed = true;
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }


}
