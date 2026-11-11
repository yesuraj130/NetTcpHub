using System;
using System.IO;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Principal;
using System.ServiceModel;
using System.ServiceModel.Security;
using System.Threading;
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
        public bool EnableEncryption { get; set; } = true;
        public string ServerIdentity { get; set; }
        public long MaxReceivedMessageSize { get; set; } = 1024 * 1024;
        public int OpenTimeoutSeconds { get; set; } = 30;
        public int SendTimeoutSeconds { get; set; } = 60;
        public int ReceiveTimeoutMinutes { get; set; } = 10;
        public int CloseTimeoutSeconds { get; set; } = 30;
        public bool AutoReconnect { get; set; } = true;
        public bool AutoDeleteTempFile { get; set; } = true;
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
        private IClientChannel clientChannel;
        private readonly bool reconnectEnabled;
        private readonly bool autoDeleteTempFile;
        private readonly NetTcpBinding binding;
        private readonly EndpointAddress address;
        private readonly bool allowNtlm;
        private readonly string serverName;

        private readonly object sync = new object();
        private volatile bool isUserDisconnecting;
        private volatile bool reconnecting;
        private int reconnectAttempts;
        private CancellationTokenSource reconnectCts = new CancellationTokenSource();
        private bool disposed;

        public bool IsConnected => Status == ConnectionStatus.Connected;
        public ConnectionStatus Status => clientChannel == null
            ? ConnectionStatus.NotConnected
            : ToConnectionStatus(clientChannel.State);

        public TcpHubClient(TcpHubClientOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(options.ServerName)) throw new ArgumentException("Server name is required.");

            serverName = options.ServerName;
            reconnectEnabled = options.AutoReconnect;
            autoDeleteTempFile = options.AutoDeleteTempFile;

            binding = new NetTcpBinding(SecurityMode.Transport);
            binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
            binding.Security.Transport.ProtectionLevel = options.EnableEncryption
                ? ProtectionLevel.EncryptAndSign
                : ProtectionLevel.Sign;
            binding.MaxReceivedMessageSize = options.MaxReceivedMessageSize;
            binding.OpenTimeout = TimeSpan.FromSeconds(options.OpenTimeoutSeconds);
            binding.SendTimeout = TimeSpan.FromSeconds(options.SendTimeoutSeconds);
            binding.ReceiveTimeout = TimeSpan.FromMinutes(options.ReceiveTimeoutMinutes);
            binding.CloseTimeout = TimeSpan.FromSeconds(options.CloseTimeoutSeconds);

            EndpointIdentity identity;
            if (!string.IsNullOrWhiteSpace(options.ServerIdentity))
            {
                identity = EndpointIdentity.CreateUpnIdentity(options.ServerIdentity);
            }
            else
            {
                identity = EndpointIdentity.CreateDnsIdentity(options.ServerName);
            }

            var serviceUri = new Uri($"net.tcp://{options.ServerName}:{options.Port}/{options.ServicePath.Trim('/')}");
            address = new EndpointAddress(serviceUri, identity);
            allowNtlm = options.AllowNtlm;
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
                catch (SecurityNegotiationException ex)
                {
                    Cleanup();
                    throw new InvalidOperationException(
                        $"Security negotiation failed with server '{serverName}'. Ensure Windows credentials, NTLM permissions, and encryption/protection levels match the server configuration.", ex);
                }
                catch (AuthenticationException ex)
                {
                    Cleanup();
                    throw new InvalidOperationException(
                        $"Authentication failed with server '{serverName}'. Ensure Windows identity has access.", ex);
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
            CancelReconnect();
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
            if (!isUserDisconnecting && reconnectEnabled)
            {
                _ = AttemptReconnect();
            }
        }

        private void Channel_Closing(object sender, EventArgs e) => OnConnectionChanged(ConnectionStatus.Disconnecting);

        private void Channel_Closed(object sender, EventArgs e)
        {
            OnConnectionChanged(ConnectionStatus.Disconnected);
            if (!isUserDisconnecting && reconnectEnabled)
            {
                _ = AttemptReconnect();
            }
        }

        public event EventHandlerExt<TcpHubClient, MessageReceivedEventArgs> MessageReceived;
        public event EventHandlerExt<TcpHubClient, FileReceivedEventArgs> FileReceived;
        public event EventHandlerExt<TcpHubClient, ConnectionChangedEventArgs> ConnectionChanged;

        public void SendMessage(HubMessage message)
        {
            EnsureConnected();
            ValidateMessage(message);
            proxy.Send(message);
        }

        public void SendFile(string filePath)
        {
            EnsureConnected();
            if (string.IsNullOrWhiteSpace(filePath)) throw new ArgumentNullException(nameof(filePath));
            if (!File.Exists(filePath)) throw new FileNotFoundException("File not found.", filePath);

            using (FileStream fs = File.OpenRead(filePath))
            {
                proxy.Upload(fs);
            }
        }

        private void ReceivedMessage(HubMessage message)
        {
            try
            {
                ValidateMessage(message);
            }
            catch
            {
                return;
            }

            OnMessageReceived(message);
        }

        private void ReceivedStream(Stream stream)
        {
            string tempFile = Path.GetTempFileName();
            try
            {
                using (var fileStream = new FileStream(tempFile, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
                {
                    stream.CopyTo(fileStream);
                }

                OnFileReceived(tempFile);
            }
            finally
            {
                if (autoDeleteTempFile && File.Exists(tempFile))
                {
                    try { File.Delete(tempFile); } catch { }
                }
            }
        }

        private void EnsureConnected()
        {
            if (!IsConnected) throw new InvalidOperationException("Client is not connected.");
        }

        private void OnMessageReceived(HubMessage message) =>
            MessageReceived?.Invoke(this, new MessageReceivedEventArgs { Message = message });

        private void OnFileReceived(string filePath) =>
            FileReceived?.Invoke(this, new FileReceivedEventArgs { FilePath = filePath });

        private void OnConnectionChanged(ConnectionStatus connectionStatus) =>
            ConnectionChanged?.Invoke(this, new ConnectionChangedEventArgs(connectionStatus));

        private async Task AttemptReconnect()
        {
            if (reconnecting || isUserDisconnecting || disposed) return;

            lock (sync)
            {
                if (reconnecting || isUserDisconnecting || disposed) return;
                reconnecting = true;
            }

            try
            {
                var token = reconnectCts.Token;
                while (!disposed && !isUserDisconnecting && !IsConnected && !token.IsCancellationRequested)
                {
                    try
                    {
                        reconnectAttempts++;
                        var delaySeconds = Math.Min((int)Math.Pow(2, Math.Min(reconnectAttempts, 6)), 60);
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds), token);

                        if (isUserDisconnecting || disposed) break;

                        Cleanup();
                        Connect();
                        reconnectAttempts = 0;
                        return;
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch
                    {
                        // Will retry on next loop
                    }
                }
            }
            finally
            {
                reconnecting = false;
            }
        }

        private void CancelReconnect()
        {
            try
            {
                reconnectCts.Cancel();
                reconnectCts.Dispose();
                reconnectCts = new CancellationTokenSource();
            }
            catch { }
        }

        private static ConnectionStatus ToConnectionStatus(CommunicationState state)
        {
            switch (state)
            {
                case CommunicationState.Created:
                    return ConnectionStatus.NotConnected;
                case CommunicationState.Opening:
                    return ConnectionStatus.Connecting;
                case CommunicationState.Opened:
                    return ConnectionStatus.Connected;
                case CommunicationState.Closing:
                    return ConnectionStatus.Disconnecting;
                case CommunicationState.Closed:
                    return ConnectionStatus.Disconnected;
                case CommunicationState.Faulted:
                    return ConnectionStatus.Faulted;
                default:
                    return ConnectionStatus.NotConnected;
            }
        }

        private static void ValidateMessage(HubMessage message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message), "Message cannot be null.");
            if (string.IsNullOrWhiteSpace(message.Data)) throw new ArgumentException("Data cannot be empty.");
            if (message.Data.Length > 100000) throw new ArgumentException("Data exceeds maximum length.");
            if (string.IsNullOrWhiteSpace(message.DataType)) throw new ArgumentException("DataType required.");
            if (message.MessageId == Guid.Empty) message.MessageId = Guid.NewGuid();
            if (message.CreatedUtc == default) message.CreatedUtc = DateTime.UtcNow;
        }

        private void Cleanup()
        {
            try
            {
                if (clientChannel != null)
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

        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing)
            {
                isUserDisconnecting = true;
                CancelReconnect();
                Cleanup();
            }
            disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
