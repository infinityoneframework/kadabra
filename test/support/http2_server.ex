defmodule Kadabra.TestServer do
  @moduledoc false
  import Bitwise

  def start do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])

    {:ok, port} = :inet.port(listener)
    acceptor = spawn_link(fn -> accept(listener) end)

    ExUnit.Callbacks.on_exit(fn ->
      Process.exit(acceptor, :kill)
      :gen_tcp.close(listener)
    end)

    "http://127.0.0.1:#{port}"
  end

  def start_tls do
    certificates =
      :public_key.pkix_test_data(%{
        root: [digest: :sha256, key: {:rsa, 2048, 65_537}],
        intermediates: [],
        peer: [
          digest: :sha256,
          key: {:rsa, 2048, 65_537},
          extensions: [
            {:Extension, {2, 5, 29, 17}, false, [{:dNSName, ~c"localhost"}]}
          ]
        ]
      })

    {:ok, listener} =
      :ssl.listen(
        0,
        certificates ++
          [
            :binary,
            active: false,
            ip: {127, 0, 0, 1},
            alpn_preferred_protocols: ["h2"]
          ]
      )

    {:ok, {_, port}} = :ssl.sockname(listener)
    acceptor = spawn_link(fn -> accept_tls(listener) end)

    ExUnit.Callbacks.on_exit(fn ->
      Process.exit(acceptor, :kill)
      :ssl.close(listener)
    end)

    {"https://localhost:#{port}", [ssl: [cacerts: certificates[:cacerts]]]}
  end

  defp accept_tls(listener) do
    case :ssl.transport_accept(listener) do
      {:ok, socket} ->
        case :ssl.handshake(socket, 5_000) do
          {:ok, socket} ->
            worker =
              spawn_link(fn ->
                receive do
                  {:socket, socket} -> serve({:ssl, socket})
                end
              end)

            :ok = :ssl.controlling_process(socket, worker)
            send(worker, {:socket, socket})

          {:error, _} ->
            :ssl.close(socket)
        end

        accept_tls(listener)

      {:error, :closed} ->
        :ok
    end
  end

  defp recv({transport, socket}, length, timeout),
    do: transport.recv(socket, length, timeout)

  defp send_data({transport, socket}, data), do: transport.send(socket, data)
  defp close({transport, socket}), do: transport.close(socket)

  defp accept(listener) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        worker =
          spawn_link(fn ->
            receive do
              {:socket, socket} -> serve({:gen_tcp, socket})
            end
          end)

        :ok = :gen_tcp.controlling_process(socket, worker)
        send(worker, {:socket, socket})
        accept(listener)

      {:error, :closed} ->
        :ok
    end
  end

  defp serve(socket) do
    with {:ok, preface} <- recv(socket, 24, 5_000),
         true <- preface == Kadabra.Socket.connection_preface() do
      send_data(socket, frame(4, 0, 0, <<>>))
      loop(socket, :hpack.new_context(), :hpack.new_context(), %{})
    end
  end

  defp loop(socket, decoder, encoder, requests) do
    with {:ok, <<length::24, type, flags, _::1, id::31>>} <-
           recv(socket, 9, 5_000),
         {:ok, payload} <- payload(socket, length) do
      case handle(socket, type, flags, id, payload, decoder, encoder, requests) do
        :closed -> close(socket)
        {decoder, encoder, requests} -> loop(socket, decoder, encoder, requests)
      end
    else
      _ -> close(socket)
    end
  end

  defp payload(_socket, 0), do: {:ok, <<>>}
  defp payload(socket, length), do: recv(socket, length, 5_000)

  defp handle(socket, 4, flags, 0, _, decoder, encoder, requests) do
    if (flags &&& 1) == 0, do: send_data(socket, frame(4, 1, 0, <<>>))
    {decoder, encoder, requests}
  end

  defp handle(socket, 6, 0, 0, payload, decoder, encoder, requests) do
    send_data(socket, frame(6, 1, 0, payload))
    {decoder, encoder, requests}
  end

  defp handle(_socket, 7, _, _, _, _, _, _), do: :closed

  defp handle(socket, 1, flags, id, payload, decoder, encoder, requests) do
    {:ok, {headers, decoder}} = :hpack.decode(payload, decoder)
    requests = Map.put(requests, id, {headers, []})
    finish(socket, flags, id, decoder, encoder, requests)
  end

  defp handle(socket, 0, flags, id, payload, decoder, encoder, requests) do
    requests =
      Map.update!(requests, id, fn {headers, body} ->
        {headers, [body, payload]}
      end)

    # Replenish both upload windows so large requests can finish.
    increment = byte_size(payload)

    if increment > 0 do
      send_data(socket, [
        frame(8, 0, 0, <<increment::32>>),
        frame(8, 0, id, <<increment::32>>)
      ])
    end

    finish(socket, flags, id, decoder, encoder, requests)
  end

  defp handle(_, _, _, _, _, decoder, encoder, requests),
    do: {decoder, encoder, requests}

  defp finish(socket, flags, id, decoder, encoder, requests) do
    if (flags &&& 1) == 1 do
      {{headers, body}, requests} = Map.pop!(requests, id)
      encoder = respond(socket, id, headers, IO.iodata_to_binary(body), encoder)
      {decoder, encoder, requests}
    else
      {decoder, encoder, requests}
    end
  end

  defp respond(socket, id, headers, body, encoder) do
    path = value(headers, ":path")
    method = value(headers, ":method")

    {status, body} =
      case {method, path} do
        {"HEAD", _} ->
          {"200", ""}

        {_, "/clockstream"} ->
          {"200", :streaming}

        {_, "/redirect"} ->
          {"302", "<a href=\"/\">Found</a>.\n\n"}

        {_, "/file/gopher.png"} ->
          {"200", :binary.copy(<<0>>, 17_668)}

        {_, "/file/go.src.tar.gz"} ->
          {"200", :binary.copy(<<0>>, 10_921_353)}

        {_, "/ECHO"} ->
          {"200", String.upcase(body)}

        {_, "/crc32"} ->
          {"200",
           "bytes=#{byte_size(body)}, CRC32=#{Integer.to_string(:erlang.crc32(body), 16) |> String.downcase()}"}

        _ ->
          {"200", "local HTTP/2 response"}
      end

    encoder =
      if path == "/serverpush" do
        {:ok, {block, encoder}} =
          :hpack.encode([{":method", "GET"}, {":path", "/pushed"}], encoder)

        send_data(socket, frame(5, 4, id, <<2::32, block::binary>>))
        encoder
      else
        encoder
      end

    {:ok, {block, encoder}} = :hpack.encode([{":status", status}], encoder)
    send_data(socket, frame(1, 4, id, block))
    if body != :streaming, do: send_body(socket, id, body)
    encoder
  end

  defp send_body(socket, id, <<chunk::binary-size(16_384), rest::binary>>)
       when rest != "" do
    send_data(socket, frame(0, 0, id, chunk))
    send_body(socket, id, rest)
  end

  defp send_body(socket, id, body),
    do: send_data(socket, frame(0, 1, id, body))

  defp value(headers, key), do: headers |> List.keyfind(key, 0) |> elem(1)

  defp frame(type, flags, id, payload),
    do: <<byte_size(payload)::24, type, flags, 0::1, id::31, payload::binary>>
end
