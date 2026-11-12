using System;
using System.IO;
using System.Net.Security;
using System.Security.Principal;
using System.ServiceModel;
using System.Threading.Tasks;

namespace NetTcpHub
{

    public class TcpHubClientOptions
    {
        public string ServerName { get; set; }
        public int Port { get; set; } = 55555;
        public string ServicePath { get; set; } = "NetTcpHub";
        public bool AllowNtlm { get; set; } = true;
        public bool EnableEncryption { get; set; } = true;
        public string ServerIdentity { get; set; }
        public long MaxReceivedMessageSize { get; set; } = 1024 * 1024;
        public int OpenTimeoutSeconds { get; set; } = 30;
        public int SendTimeoutSeconds { get; set; } = 60;
        public int ReceiveTimeoutMinutes { get; set; } = 10;
        public int CloseTimeoutSeconds { get; set; } = 30;

        public bool AutoReconnect { get; set; } = true;
    }

    public class MessageReceivedEventArgs : EventArgs
    {
        public MessageReceivedEventArgs(string userName, string sessionId, HubMessage message)
        {
            UserName = userName;
            SessionId = sessionId;
            Message = message;
        }

        public string UserName { get; }
        public string SessionId { get; }
        public HubMessage Message { get; }
    }

    public class FileReceivedEventArgs : EventArgs
    {
        public FileReceivedEventArgs(string sessionId, string filePath, bool autoDelete)
        {
            SessionId = sessionId;
            FilePath = filePath;
            AutoDelete = autoDelete;
        }

        public string SessionId { get; }
        public string FilePath { get; }
        public bool AutoDelete { get; set; }
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

#pragma warning disable CA2213 // Disposable fields should be disposed
        private DuplexChannelFactory<IServerMethods> channelFactory;
#pragma warning restore CA2213 // Disposable fields should be disposed
        private IServerMethods proxy;
        private IClientChannel clientChannel;

        private readonly bool reconnectEnabled;
        private readonly NetTcpBinding binding;
        private readonly string serverName;
        private readonly bool allowNtlm;
        private readonly EndpointAddress address;

        private readonly object sync = new object();

        public bool IsConnected => Status == ConnectionStatus.Connected;
        public ConnectionStatus Status => clientChannel is null ? ConnectionStatus.NotConnected : clientChannel.State.ToConnectionStatus();
        //public string Username => ConnectionInfo?.UserIdentity;

        public TcpHubClient(TcpHubClientOptions options)
        {
            if (options is null) throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.ServerName)) throw new ArgumentException("Server name is required.");

            binding = new NetTcpBinding(SecurityMode.Transport);
            binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
            binding.Security.Transport.ProtectionLevel = options.EnableEncryption ? ProtectionLevel.EncryptAndSign : ProtectionLevel.Sign; ;
            binding.MaxReceivedMessageSize = options.MaxReceivedMessageSize;
            binding.OpenTimeout = TimeSpan.FromSeconds(options.OpenTimeoutSeconds);
            binding.SendTimeout = TimeSpan.FromSeconds(options.SendTimeoutSeconds);
            binding.ReceiveTimeout = TimeSpan.FromMinutes(options.ReceiveTimeoutMinutes);
            binding.CloseTimeout = TimeSpan.FromSeconds(options.CloseTimeoutSeconds);

            reconnectEnabled = options.AutoReconnect;
            allowNtlm = options.AllowNtlm;
            serverName = options.ServerName;

            //binding.ReliableSession.Enabled = true;
            //binding.ReliableSession.InactivityTimeout = TimeSpan.FromMinutes(5);

            EndpointIdentity identity = !string.IsNullOrWhiteSpace(options.ServerIdentity) ? EndpointIdentity.CreateUpnIdentity(options.ServerIdentity) : EndpointIdentity.CreateDnsIdentity(options.ServerName);
            //EndpointIdentity identity = EndpointIdentity.CreateUpnIdentity(UserPrincipal.Current.UserPrincipalName);
            var serviceUri = new Uri($"net.tcp://{options.ServerName}:{options.Port}/{options.ServicePath.Trim('/')}");
            address = new EndpointAddress(serviceUri, identity);
        }


        public void Connect()
        {
            if (disposed) return;

            lock (sync)
            {
                if (IsConnected) return;
                isUserDisconnecting = false;

                var context = new InstanceContext(new TcpHubClientMethods(this));
                channelFactory = new DuplexChannelFactory<IServerMethods>(context, binding, address);
#pragma warning disable CS0618 // Type or member is obsolete
                if (!allowNtlm) channelFactory.Credentials.Windows.AllowNtlm = false;
#pragma warning restore CS0618 // Type or member is obsolete
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
                catch (System.ServiceModel.Security.SecurityNegotiationException ex)
                {
                    Cleanup();
                    throw new InvalidOperationException($"Security negotiation failed with server '{serverName}'. Ensure Windows credentials, NTLM permissions, and encryption/protection levels match the server configuration.", ex);
                }
                catch (System.Security.Authentication.AuthenticationException ex)
                {
                    Cleanup();
                    throw new InvalidOperationException($"Authentication failed with server '{serverName}'. Ensure Windows identity has access.", ex);
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
            isUserDisconnecting = true;
            Cleanup();
            OnConnectionChanged(ConnectionStatus.Disconnected);
        }

        private void Channel_Opening(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Connecting);
        private void Channel_Opened(object sender, EventArgs e)
        {
            reconnectAttempts = 0;
            OnConnectionChanged(ConnectionStatus.Connected);
        }
        private void Channel_Faulted(object sender, EventArgs e)
        {
            OnConnectionChanged(ConnectionStatus.Faulted);
            if (!isUserDisconnecting && reconnectEnabled) _ = AttemptReconnect();
        }
        private void Channel_Closing(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Disconnecting);
        private void Channel_Closed(object sender, EventArgs e)
        {
            OnConnectionChanged(ConnectionStatus.Disconnected);
            if (!isUserDisconnecting && reconnectEnabled) _ = AttemptReconnect();
        }

#pragma warning disable CA1003 // Use generic event handler instances
        public event EventHandlerExt<TcpHubClient, MessageReceivedEventArgs> MessageReceived;
        public event EventHandlerExt<TcpHubClient, FileReceivedEventArgs> FileReceived;
        public event EventHandlerExt<TcpHubClient, ConnectionChangedEventArgs> ConnectionChanged;
#pragma warning restore CA1003 // Use generic event handler instances

        private void OnMessageReceived(HubMessage message) => MessageReceived?.Invoke(this, new MessageReceivedEventArgs(null, null, message));
        private void OnFileReceived(FileReceivedEventArgs eventArgs) => FileReceived?.Invoke(this, eventArgs);
        private void OnConnectionChanged(ConnectionStatus connectionStatus) => ConnectionChanged?.Invoke(this, new ConnectionChangedEventArgs(connectionStatus));
        
        public bool SendMessage(HubMessage message)
        {
            if (!IsConnected) return false;
            proxy.Send(message);
            return true;
        }
        public bool SendFile(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath)) throw new ArgumentNullException(nameof(filePath));
            if (!File.Exists(filePath)) throw new FileNotFoundException("File not found.", filePath);

            if (!IsConnected) return false;
            using (var fileStream = File.OpenRead(filePath))
            {
                proxy.Upload(fileStream);
            }
            return true;
        }

        private void ReceivedMessage(HubMessage message)
        {
            message.ValidateMessage();
            OnMessageReceived(message);
        }
        private void ReceivedStream(Stream stream)
        {
            string tempFile = Path.GetTempFileName();
            var eventArgs = new FileReceivedEventArgs(null, tempFile, true);
            try
            {
                using (var fileStream = new FileStream(tempFile, FileMode.Open, FileAccess.ReadWrite))
                {
                    stream.CopyTo(fileStream);
                }

                OnFileReceived(eventArgs);
            }
            finally
            {
                if (eventArgs.AutoDelete && File.Exists(tempFile)) File.Delete(tempFile);
            }
        }

        private int reconnectAttempts;
        private volatile bool reconnecting;
        private volatile bool isUserDisconnecting;
        private async Task AttemptReconnect()
        {
            //if (!reconnectEnabled) return;
            if (reconnecting || isUserDisconnecting || disposed) return;


            lock (sync)
            {
                if (reconnecting || isUserDisconnecting || disposed) return;
                reconnecting = true;
            }

            try
            {
                //status = ConnectionStatus.Reconnecting;

                while (!disposed && !isUserDisconnecting && !IsConnected)
                {
                    try
                    {
                        reconnectAttempts++;
                        var delaySeconds = Math.Min((int)Math.Pow(2, reconnectAttempts), 60);
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds));

                        if (isUserDisconnecting || disposed || IsConnected) break;

                        Cleanup();
                        Connect();

                        reconnectAttempts = 0;
                        return;
                    }
                    catch { } // Will retry on next loop
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
                if (clientChannel is not null)
                {
                    clientChannel.Opening -= Channel_Opening;
                    clientChannel.Opened -= Channel_Opened;
                    clientChannel.Faulted -= Channel_Faulted;
                    clientChannel.Closing -= Channel_Closing;
                    clientChannel.Closed -= Channel_Closed;

                    if (clientChannel.State == CommunicationState.Faulted) clientChannel.Abort();
                    else clientChannel.Close();
                }
            }
            catch { clientChannel?.Abort(); }
            clientChannel = null;
            proxy = null;

            try
            {
                if (channelFactory is not null)
                {
                    if (channelFactory.State == CommunicationState.Faulted) channelFactory.Abort();
                    else channelFactory.Close();
                }
            }
            catch { channelFactory?.Abort(); }
            channelFactory = null;
        }

        private bool disposed;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing)
            {
                disposed = true;
                isUserDisconnecting = true;
                Cleanup();
            }
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
