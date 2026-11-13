using System;
using System.IO;
using System.Net.Security;
using System.Security.Principal;
using System.ServiceModel;
using System.ServiceModel.Dispatcher;
using System.Text;
using System.Threading.Tasks;

namespace NetTcpHub
{
    internal interface IClientMethods
    {
        [OperationContract(IsOneWay = true)] void Send(HubMessage message);
        [OperationContract(IsOneWay = true)] void Upload(FileUploadRequest stream);
    }

    public class TcpHubClient : IDisposable
    {
        #region Private Classes
        private class TcpHubClientMethods : IClientMethods
        {
            private readonly TcpHubClient tcpHubClient;
            public TcpHubClientMethods(TcpHubClient tcpHubClient) => this.tcpHubClient = tcpHubClient;

            public void Send(HubMessage message) => tcpHubClient.ReceivedMessage(message);
            public void Upload(FileUploadRequest stream) => tcpHubClient.ReceivedStream(stream);
        }

        private class TcpHubSessionShutdown : IInputSessionShutdown
        {
            private readonly TcpHubClient tcpHubClient;
            public TcpHubSessionShutdown(TcpHubClient tcpHubClient) => this.tcpHubClient = tcpHubClient;

            public void ChannelFaulted(IDuplexContextChannel channel) { if (channel.State == CommunicationState.Opened) tcpHubClient.CleanUp(); }
            public void DoneReceiving(IDuplexContextChannel channel) { if (channel.State == CommunicationState.Opened) tcpHubClient.CleanUp(); }
        }
        #endregion


        #region Fields
#pragma warning disable CA2213 // Disposable fields should be disposed
        private DuplexChannelFactory<IServerMethods> channelFactory;
#pragma warning restore CA2213 // Disposable fields should be disposed
        private IServerMethods proxy;
        private IClientChannel clientChannel;

        private readonly TcpHubClientOptions options;
        private readonly NetTcpBinding binding;
        private readonly EndpointAddress address;
        private readonly object sync = new object();
        #endregion


        #region Constructor
        private TcpHubClient(TcpHubClientOptions clientOptions)
        {
            this.options = clientOptions;

            binding = new NetTcpBinding(SecurityMode.Transport);
            binding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
            binding.Security.Transport.ProtectionLevel = clientOptions.EnableEncryption ? ProtectionLevel.EncryptAndSign : ProtectionLevel.Sign;
            binding.MaxReceivedMessageSize = clientOptions.MaxReceivedMessageSize;
            binding.OpenTimeout = TimeSpan.FromSeconds(clientOptions.OpenTimeoutSeconds);
            binding.SendTimeout = TimeSpan.FromSeconds(clientOptions.SendTimeoutSeconds);
            binding.ReceiveTimeout = TimeSpan.FromMinutes(clientOptions.ReceiveTimeoutMinutes);
            binding.CloseTimeout = TimeSpan.FromSeconds(clientOptions.CloseTimeoutSeconds);
            //binding.ReliableSession.Enabled = true;
            //binding.ReliableSession.InactivityTimeout = TimeSpan.FromMinutes(5);

            var identity = !string.IsNullOrWhiteSpace(clientOptions.UserIdentity) ? EndpointIdentity.CreateUpnIdentity(clientOptions.UserIdentity) : EndpointIdentity.CreateDnsIdentity(clientOptions.ServerName);
            var serviceUri = new Uri($"net.tcp://{clientOptions.ServerName}:{clientOptions.Port}/{clientOptions.ServicePath.Trim('/')}");
            address = new EndpointAddress(serviceUri, identity);
        }
        public static TcpHubClient Create(TcpHubClientOptions clientOptions, out string error)
        {
            if (!ValidateClientOptions(clientOptions, out error)) return null;
            return new TcpHubClient(clientOptions);
        }
        private static bool ValidateClientOptions(TcpHubClientOptions clientOptions, out string error)
        {
            if (clientOptions is null) { error = $"{nameof(clientOptions)} cannot be null"; return false; }
            if (string.IsNullOrWhiteSpace(clientOptions.ServerName)) { error = $"{nameof(clientOptions.ServerName)} cannot be empty"; return false; }
            if (clientOptions.Port < 1000 || clientOptions.Port > 65535) { error = $"{nameof(clientOptions.Port)} must be between 1000 and 65535"; return false; }
            if (string.IsNullOrWhiteSpace(clientOptions.ServicePath)) { error = $"{nameof(clientOptions.ServicePath)} cannot be empty"; return false; }
            error = null; return true;
        }
        #endregion


        #region Public
        public bool IsConnected => Status == ConnectionStatus.Connected;
        public ConnectionStatus Status => clientChannel is null ? ConnectionStatus.NotConnected : clientChannel.State.ToConnectionStatus();

        public bool Connect(out string error) => ConnectInternal(false, out error);
        private bool ConnectInternal(bool isReconnect, out string error)
        {
            if (disposed) { error = $"already disposed"; return false; }

            lock (sync)
            {
                if (disposed) { error = $"already disposed"; return false; }
                if (isReconnect && isUserDisconnecting) { error = "disconnected by user"; return false; }
                if (IsConnected) { error = $"already connected"; return false; }
                if (!isReconnect) isUserDisconnecting = false;

                try
                {
                    var context = new InstanceContext(new TcpHubClientMethods(this));
                    channelFactory = new DuplexChannelFactory<IServerMethods>(context, binding, address);
#pragma warning disable CS0618 // Type or member is obsolete
                    if (!options.AllowNtlm) channelFactory.Credentials.Windows.AllowNtlm = false;
#pragma warning restore CS0618 // Type or member is obsolete
                    channelFactory.Credentials.Windows.AllowedImpersonationLevel = TokenImpersonationLevel.Identification;
                    channelFactory.Endpoint.EndpointBehaviors.Add(new ClientInputSessionShutdownBehavior(new TcpHubSessionShutdown(this)));

                    proxy = channelFactory.CreateChannel();
                    clientChannel = (IClientChannel)proxy;

                    clientChannel.Opening += Channel_Opening;
                    clientChannel.Opened += Channel_Opened;
                    clientChannel.Faulted += Channel_Faulted;
                    clientChannel.Closing += Channel_Closing;
                    clientChannel.Closed += Channel_Closed;

                    clientChannel.Open();
                    error = null; return true;
                }
                catch (System.ServiceModel.Security.SecurityNegotiationException ex)
                {
                    CleanUp();
                    error = $"Security negotiation failed with server '{options.ServerName}'. Ensure Windows credentials, NTLM permissions, and encryption/protection levels match the server configuration..\n\n{ ex}";
                    return false;
                }
                catch (System.Security.Authentication.AuthenticationException ex)
                {
                    CleanUp();
                    error = $"Authentication failed with server '{options.ServerName}'. Ensure Windows identity has access.\n\n{ex}";
                    return false;
                }
                catch (EndpointNotFoundException ex)
                {
                    CleanUp();
                    error = $"Server not found '{options.ServerName}'. Check server status.\n\n{ex}";
                    return false;
                }
                catch (Exception ex)
                {
                    CleanUp();
                    error = $"{ex.Message}.\n\n{ex}";
                    return false;
                }
            }


        }
        public void Disconnect()
        {
            isUserDisconnecting = true;
            lock (sync) { CleanUp(); }
        }
        #endregion


        #region Events
        private void Channel_Opening(object sender, EventArgs e) => OnConnectionChanged(((IContextChannel)sender)?.SessionId, ConnectionStatus.Connecting);
        private void Channel_Opened(object sender, EventArgs e) { reconnectAttempts = 0; OnConnectionChanged(((IContextChannel)sender)?.SessionId, ConnectionStatus.Connected); }
        private void Channel_Closing(object sender, EventArgs e) => OnConnectionChanged(((IContextChannel)sender)?.SessionId, ConnectionStatus.Disconnecting);
        private void Channel_Closed(object sender, EventArgs e)
        {
            if (clientChannel is not null)
            {
                clientChannel.Opening -= Channel_Opening;
                clientChannel.Opened -= Channel_Opened;
                clientChannel.Faulted -= Channel_Faulted;
                clientChannel.Closing -= Channel_Closing;
                clientChannel.Closed -= Channel_Closed;
            }

            OnConnectionChanged(((IContextChannel)sender)?.SessionId, ConnectionStatus.Disconnected);
            if (!isUserDisconnecting && options.AutoReconnect) _ = AttemptReconnect();
        }
        private void Channel_Faulted(object sender, EventArgs e)
        {
            if (clientChannel is not null)
            {
                clientChannel.Opening -= Channel_Opening;
                clientChannel.Opened -= Channel_Opened;
                clientChannel.Faulted -= Channel_Faulted;
                clientChannel.Closing -= Channel_Closing;
                clientChannel.Closed -= Channel_Closed;
            }

            lock (sync) { CleanUp(); }
            OnConnectionChanged(((IContextChannel)sender)?.SessionId, ConnectionStatus.Faulted);
            if (!isUserDisconnecting && options.AutoReconnect) _ = AttemptReconnect();
        }

#pragma warning disable CA1003 // Use generic event handler instances
        public event EventHandlerExt<TcpHubClient, MessageReceivedEventArgs> MessageReceived;
        public event EventHandlerExt<TcpHubClient, FileReceivedEventArgs> FileReceived;
        public event EventHandlerExt<TcpHubClient, ConnectionChangedEventArgs> ConnectionChanged;
#pragma warning restore CA1003 // Use generic event handler instances

        // Note: Event handlers execute synchronously on WCF threads; subscribers should avoid blocking or throwing unhandled exceptions.
        private void OnMessageReceived(HubMessage message) => MessageReceived?.Invoke(this, new MessageReceivedEventArgs(null, null, message));
        private void OnFileReceived(FileReceivedEventArgs eventArgs) => FileReceived?.Invoke(this, eventArgs);
        private void OnConnectionChanged(string sessionId, ConnectionStatus connectionStatus) => ConnectionChanged?.Invoke(this, new ConnectionChangedEventArgs(sessionId, connectionStatus));
        #endregion


        #region Send and Receive
        private bool ValidateMessage(HubMessage message, out string error)
        {
            if (!message.ValidateMessage(out error)) return false;
            if (Encoding.UTF8.GetByteCount(message.Data) > options.MaxReceivedMessageSize) { error = $"{nameof(message.Data)} cannot be more than {options.MaxReceivedMessageSize}"; return false; }
            return true;
        }
        public bool SendMessage(HubMessage message, out string error)
        {
            var currentProxy = proxy;
            if (!IsConnected || currentProxy is null) { error = "Not connected"; return false; }
            if (!ValidateMessage(message, out error)) return false;

            try
            {
                currentProxy.Send(message);
                return true;
            }
            catch (Exception ex)
            {
                error = $"{ex.Message}.\n\n{ex}";
                return false;
            }
        }
        public bool SendFile(string filePath, HubMessage message, out string error)
        {
            var currentProxy = proxy;
            if (!IsConnected || currentProxy is null) { error = "Not connected"; return false; }
            if (!ValidateMessage(message, out error)) return false;
            if (string.IsNullOrWhiteSpace(filePath) || !File.Exists(filePath)) { error = "File not exists"; return false; }

            try
            {
                using (var fileStream = File.OpenRead(filePath))
                {
                    if (fileStream.Length + Encoding.UTF8.GetByteCount(message.Data) > options.MaxReceivedMessageSize) { error = $"File and message size cannot be more than {options.MaxReceivedMessageSize}"; return false; }
                    currentProxy.Upload(new FileUploadRequest(message, Path.GetFileName(filePath), fileStream.Length, fileStream));
                }
                return true;
            }
            catch (Exception ex)
            {
                error = $"{ex.Message}.\n\n{ex}";
                return false;
            }
        }

        private bool ReceivedMessage(HubMessage message)
        {
            if (!message.ValidateMessage(out var error)) { OnMessageReceived(new HubMessage(message?.MessageId ?? Guid.NewGuid(), "Error", error)); return false; }
            else { OnMessageReceived(message); return true; }
        }
        private bool ReceivedStream(FileUploadRequest stream)
        {
            if (stream is null || !stream.HubMessage.ValidateMessage(out var error))
            {
                stream?.FileStream?.Dispose();
                OnFileReceived(new FileReceivedEventArgs(null, null, new HubMessage(stream?.HubMessage?.MessageId ?? Guid.NewGuid(), "Error", stream is null ? $"{nameof(stream)} is null" : error), true));
                return false;
            }
            if (stream.FileStream is null) { OnFileReceived(new FileReceivedEventArgs(null, null, new HubMessage(stream.HubMessage.MessageId, "Error", $"{nameof(stream.FileStream)} is null"), true)); return false; }

            var tempFile = Path.Combine(Path.GetTempPath(), $"NetTcpHub_{Guid.NewGuid():N}.tmp");
            var eventArgs = new FileReceivedEventArgs(null, tempFile, stream.HubMessage, true, Path.GetFileName(stream.FileName));
            try
            {
                using (stream.FileStream)
                {
                    try
                    {
                        using (var fileStream = new FileStream(tempFile, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 81920, FileOptions.SequentialScan))
                        {
                            stream.FileStream.CopyTo(fileStream);
                        }
                        OnFileReceived(eventArgs); return true;
                    }
                    catch (Exception ex)
                    {
                        OnFileReceived(new FileReceivedEventArgs(null, null, new HubMessage(stream.HubMessage.MessageId, "Error", $"{ex.Message}.\n\n{ex}"), true)); return false;
                    }
                }
            }
            finally
            {
                if (eventArgs.AutoDelete && File.Exists(tempFile)) File.Delete(tempFile);
            }
        }
        #endregion


        #region Reconnect and Dispose
        private int reconnectAttempts;
        private volatile bool reconnecting;
        private volatile bool isUserDisconnecting;
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
                //status = ConnectionStatus.Reconnecting;

                while (!disposed && !isUserDisconnecting && !IsConnected)
                {
                    reconnectAttempts++;
                    var delaySeconds = Math.Min(1 << Math.Min(reconnectAttempts, 6), 60);
                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds));

                    if (isUserDisconnecting || disposed || IsConnected) break;

                    lock (sync) { CleanUp(); }
                    if (ConnectInternal(true, out var _))
                    {
                        reconnectAttempts = 0;
                        return;
                    }
                }
            }
            finally
            {
                reconnecting = false;
            }
        }
        private void CleanUp()
        {
            if (clientChannel is not null)
            {
                try
                {
                    if (clientChannel.State == CommunicationState.Faulted) clientChannel.Abort();
                    else clientChannel.Close();
                }
                catch { clientChannel?.Abort(); }
                finally { clientChannel = null; }
            }

            if (proxy is not null) proxy = null;

            if (channelFactory is not null)
            {
                try
                {
                    if (channelFactory.State == CommunicationState.Faulted) channelFactory.Abort();
                    else channelFactory.Close();
                }
                catch { channelFactory?.Abort(); }
                finally { channelFactory = null; }
            }
        }

        private volatile bool disposed;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing)
            {
                isUserDisconnecting = true;
                lock (sync)
                {
                    if (disposed) return;
                    disposed = true;
                    CleanUp();
                }
            }
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
