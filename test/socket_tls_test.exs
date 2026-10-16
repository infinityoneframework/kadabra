defmodule Kadabra.SocketTLSTest do
  use ExUnit.Case

  test "HTTP/2 works over verified TLS with a custom trust store" do
    {uri, options} = Kadabra.TestServer.start_tls()
    assert {:ok, client} = Kadabra.open(uri, options)
    connection = :sys.get_state(client).connection
    socket_server = :sys.get_state(connection).config.socket
    socket = :sys.get_state(socket_server).socket
    assert {:ok, "h2"} = :ssl.negotiated_protocol(socket)
    assert :ok = Kadabra.get(client, "/reqinfo")

    assert_receive {:end_stream, %{status: 200, body: "local HTTP/2 response"}},
                   5_000

    Kadabra.close(client)
  end

  test "default TLS verification rejects an untrusted server" do
    {uri, _options} = Kadabra.TestServer.start_tls()
    assert {:error, reason} = Kadabra.open(uri)
    assert inspect(reason) =~ "unknown_ca"
  end

  test "TLS verification rejects a hostname mismatch" do
    {uri, options} = Kadabra.TestServer.start_tls()

    options =
      put_in(options, [:ssl, :server_name_indication], ~c"wrong.example")

    assert {:error, reason} = Kadabra.open(uri, options)
    assert inspect(reason) =~ "hostname_check_failed"
  end
end
