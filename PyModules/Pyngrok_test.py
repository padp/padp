from sshtunnel import SSHTunnelForwarder

server = SSHTunnelForwarder('https://padp.github.io/', ssh_username='testing', ssh_password='test', remote_bind_address=('127.0.0.1', 3010))
server.start()
print(server)
_=0
server.stop()