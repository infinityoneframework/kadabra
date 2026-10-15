defmodule KadabraTest do
  use ExUnit.Case, async: true

  @moduletag report: [:pid]

  alias Kadabra.Stream

  setup do
    uri = Kadabra.TestServer.start()
    {:ok, pid} = Kadabra.open(uri)

    [conn: pid, uri: uri]
  end

  describe "open/2" do
    test "sets port if specified", context do
      port = URI.parse(context.uri).port
      opts = [port: port]
      {:ok, pid} = Kadabra.open(context.uri, opts)

      conn_pid = :sys.get_state(pid).connection
      conn = :sys.get_state(conn_pid)

      assert conn.config.opts[:port] == port
      Kadabra.close(pid)
    end
  end

  describe "request/2" do
    test "can take a list of requests", context do
      headers = [
        {":method", "GET"},
        {":path", "/"}
      ]

      request = Kadabra.Request.new(headers: headers)

      Kadabra.request(context[:conn], [request, request])

      for x <- [1, 3] do
        assert_receive {:end_stream, %Stream.Response{id: ^x}}, 5_000
      end
    end

    test "can take a single request", context do
      headers = [
        {":method", "GET"},
        {":path", "/"}
      ]

      request = Kadabra.Request.new(headers: headers)

      Kadabra.request(context[:conn], request)

      assert_receive {:end_stream, %Stream.Response{id: 1}}, 5_000
    end
  end

  describe "close/1" do
    test "sends close message and stops supervisor", context do
      pid = context[:conn]
      ref = Process.monitor(pid)
      Kadabra.close(pid)

      assert_receive {:closed, ^pid}, 5_000
      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}, 5_000
    end
  end

  test "ping receives a pong from the local peer", %{conn: conn} do
    connection = :sys.get_state(conn).connection
    Kadabra.ping(conn)
    assert_receive {:pong, ^connection}, 5_000
  end

  test "HEAD, POST, and DELETE requests complete", %{conn: conn} do
    for {method, id} <- [head: 1, post: 3, delete: 5] do
      assert :ok = apply(Kadabra, method, [conn, "/"])

      assert_receive {:end_stream,
                      %Stream.Response{id: ^id, status: 200} = response},
                     5_000

      if method == :head, do: assert(response.body == "")
    end
  end

  describe "GET" do
    test "can take an options keyword list", context do
      headers = [
        {":method", "GET"},
        {":path", "/"}
      ]

      Kadabra.request(context[:conn], headers: headers, on_response: & &1)

      assert_receive {:end_stream, %Stream.Response{id: 1}}, 5_000
    end

    test "https://http2.golang.org/reqinfo", context do
      Kadabra.get(context[:conn], "/reqinfo")

      receive do
        {:end_stream, response} ->
          assert response.id == 1
          assert response.status == 200
      after
        5_000 ->
          flunk("No stream response received.")
      end
    end

    test "https://http2.golang.org/reqinfo a lot", context do
      count = 5_000

      for _x <- 1..count do
        Kadabra.get(context[:conn], "/reqinfo")
      end

      is_odd = fn x -> rem(x, 2) == 1 end
      streams = 1..(2 * count) |> Enum.filter(&is_odd.(&1))

      for x <- streams do
        assert_receive {:end_stream, %Stream.Response{id: ^x}}, 30_000
      end
    end

    test "https://http2.golang.org/redirect", context do
      Kadabra.get(context[:conn], "/redirect")

      expected_body = "<a href=\"/\">Found</a>.\n\n"
      expected_status = 302

      receive do
        {:end_stream, response} ->
          assert response.id == 1
          assert response.status == expected_status
          assert response.body == expected_body
      after
        5_000 ->
          flunk("No stream response received.")
      end
    end

    test "https://http2.golang.org/file/gopher.png", context do
      Kadabra.get(context[:conn], "/file/gopher.png")

      receive do
        {:end_stream, response} ->
          assert response.id == 1
          assert response.status == 200
          assert byte_size(response.body) == 17_668
      after
        5_000 ->
          flunk("No stream response received.")
      end
    end

    test "https://http2.golang.org/file/go.src.tar.gz", context do
      Kadabra.get(context[:conn], "/file/go.src.tar.gz")

      receive do
        {:end_stream, response} ->
          assert response.id == 1
          assert response.status == 200
          assert byte_size(response.body) == 10_921_353

        other ->
          flunk("Unexpected response: #{inspect(other)}")
      after
        45_000 ->
          flunk("No stream response received.")
      end
    end

    test "https://http2.golang.org/serverpush", context do
      Kadabra.get(context[:conn], "/serverpush")

      receive do
        {:push_promise, response} ->
          assert response.id == 2
          refute response.status
          assert Stream.Response.get_header(response.headers, ":path")
      after
        5_000 ->
          flunk("No push promise received.")
      end
    end
  end

  describe "PUT" do
    test "https://http2.golong.org/ECHO", context do
      payload = String.duplicate("test", 10)
      Kadabra.put(context[:conn], "/ECHO", body: payload)

      expected_body = String.upcase(payload)

      receive do
        {:end_stream, response} ->
          assert response.id == 1
          assert response.status == 200
          assert response.body == expected_body
      after
        15_000 ->
          flunk("No stream response received.")
      end
    end

    test "https://http2.golong.org/ECHO with large payload", context do
      payload = String.duplicate("test", 1_000_000)
      Kadabra.put(context[:conn], "/ECHO", body: payload)

      expected_body = String.upcase(payload)

      receive do
        {:end_stream, response} ->
          assert response.id == 1
          assert response.status == 200
          assert response.body == expected_body
      after
        45_000 ->
          flunk("No stream response received.")
      end
    end

    test "https://http2.golong.org/crc32", context do
      payload = "test"
      Kadabra.put(context[:conn], "/crc32", body: payload)

      expected_body = "bytes=4, CRC32=d87f7e0c"

      receive do
        {:end_stream, response} ->
          assert response.id == 1
          assert response.status == 200
          assert response.body == expected_body
      after
        5_000 ->
          flunk("No stream response received.")
      end
    end
  end

  test "socket close message closes connection", %{uri: uri} do
    pid =
      uri
      |> Kadabra.open()
      |> elem(1)

    ref = Process.monitor(pid)

    conn_pid = :sys.get_state(pid).connection
    socket_pid = :sys.get_state(conn_pid).config.socket

    send(socket_pid, {:ssl_closed, nil})

    assert_receive {:closed, ^pid}, 5_000
    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}, 5_000
  end

  # @tag :golang
  # test "handles extremely large headers", _context do
  #   pid =
  #     "https://www.google.com"
  #     |> Kadabra.open()
  #     |> elem(1)

  #   big_str = String.duplicate("test", 5_000_000)

  #   # val = :hpack_integer.encode(byte_size(str), 7) |> IO.inspect(label: "size")

  #   request =
  #     Kadabra.Request.new(
  #       headers: [
  #         {":method", "GET"},
  #         {":path", "/"},
  #         # {"whatever", "yeah"}
  #         {"whatever", big_str}
  #       ]
  #     )

  #   Kadabra.request(pid, request)

  #   assert_receive {:end_stream, %Stream.Response{id: 1}}, 15_000
  # end
end
