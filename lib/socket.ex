defmodule Kadabra.Socket do
  @moduledoc false

  defstruct socket: nil, transport: :gen_tcp, buffer: "", active_user: nil

  alias Kadabra.FrameParser

  import Kernel, except: [send: 2]

  use GenServer

  @type ssl_sock :: :ssl.sslsocket()

  @type connection_result ::
          {:ok, ssl_sock}
          | {:ok, pid}
          | {:error, :not_implmenented}
          | {:error, :bad_scheme}

  @spec connection_preface() :: String.t()
  def connection_preface, do: "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

  def send(pid, bin) do
    GenServer.call(pid, {:send, bin})
  end

  def set_active(pid) do
    GenServer.call(pid, {:set_active, self()})
  end

  def start_link(uri, opts) do
    GenServer.start_link(__MODULE__, {uri, opts})
  end

  def init({uri, opts}) do
    case connect(uri, opts) do
      {:ok, socket} ->
        transport = if uri.scheme == "https", do: :ssl, else: :gen_tcp
        socket_send(socket, connection_preface(), transport)
        {:ok, %__MODULE__{socket: socket, transport: transport}}

      error ->
        error
    end
  end

  @spec connect(URI.t(), Keyword.t()) :: connection_result
  defp connect(uri, opts) do
    case uri.scheme do
      "http" -> do_connect(uri, :http, opts)
      "https" -> do_connect(uri, :https, opts)
      _ -> {:error, :bad_scheme}
    end
  end

  defp do_connect(uri, :http, opts) do
    tcp_opts =
      opts
      |> Keyword.get(:tcp, [])
      |> options(:http)

    uri.host
    |> to_charlist()
    |> :gen_tcp.connect(uri.port, tcp_opts)
  end

  defp do_connect(uri, :https, opts) do
    :ssl.start()

    ssl_opts =
      opts
      |> Keyword.get(:ssl, [])
      |> options(:https)

    uri.host
    |> to_charlist()
    |> :ssl.connect(uri.port, ssl_opts)
  end

  @spec options(Keyword.t(), :http | :https) :: [...]
  defp options(opts, :https) do
    defaults = [
      {:active, :once},
      {:packet, :raw},
      {:reuseaddr, false},
      {:verify, :verify_peer},
      {:depth, 99},
      {:cacerts, :certifi.cacerts()},
      {:alpn_advertised_protocols, [<<"h2">>]},
      :binary
    ]

    Enum.reject(defaults, fn
      {key, _} -> List.keymember?(opts, key, 0)
      _ -> false
    end) ++ opts
  end

  defp options(opts, :http) do
    defaults = [
      {:active, :once},
      {:packet, :raw},
      {:reuseaddr, false},
      :binary
    ]

    Enum.reject(defaults, fn
      {key, _} -> List.keymember?(opts, key, 0)
      _ -> false
    end) ++ opts
  end

  # Frame recv and parsing

  defp do_recv_bin(bin, %{socket: socket} = state) do
    bin = state.buffer <> bin

    case parse_bin(socket, bin, state) do
      {:unfinished, bin, state} ->
        setopts(state.socket, [{:active, :once}], state.transport)
        {:noreply, %{state | buffer: bin}}
    end
  end

  def parse_bin(_socket, bin, %{active_user: nil} = state) do
    {:unfinished, bin, state}
  end

  def parse_bin(socket, bin, state) do
    case FrameParser.parse(bin) do
      {:ok, frame, rest} ->
        Kernel.send(state.active_user, {:recv, frame})
        parse_bin(socket, rest, state)

      {:error, bin} ->
        {:unfinished, bin, state}
    end
  end

  # Internal socket helpers

  defp socket_send(socket, bin, transport), do: transport.send(socket, bin)

  defp setopts(socket, opts, :ssl), do: :ssl.setopts(socket, opts)
  defp setopts(socket, opts, :gen_tcp), do: :inet.setopts(socket, opts)

  # handle_call

  def handle_call({:set_active, pid}, _from, state) do
    {:reply, :ok, %{state | active_user: pid}}
  end

  # Ignore if socket isn't established
  def handle_call({:send, _bin}, _from, %{socket: nil} = state) do
    {:reply, :ok, state}
  end

  def handle_call({:send, bin}, _from, state) when is_binary(bin) do
    resp = socket_send(state.socket, bin, state.transport)
    {:reply, resp, state}
  end

  def handle_call({:send, bins}, _from, state) when is_list(bins) do
    for bin <- bins, do: socket_send(state.socket, bin, state.transport)
    {:reply, :ok, state}
  end

  # handle_info

  def handle_info(:send_preface, state) do
    socket_send(state.socket, connection_preface(), state.transport)
    {:noreply, state}
  end

  def handle_info({:tcp, _socket, bin}, state) do
    do_recv_bin(bin, state)
  end

  def handle_info({:tcp_closed, _socket}, state) do
    Kernel.send(state.active_user, {:closed, self()})
    {:noreply, %{state | socket: nil}}
  end

  def handle_info({:ssl, _socket, bin}, state) do
    do_recv_bin(bin, state)
  end

  def handle_info({:ssl_closed, _socket}, state) do
    Kernel.send(state.active_user, {:closed, self()})
    {:noreply, %{state | socket: nil}}
  end

  def handle_info({:ssl_error, _socket, _reason}, state) do
    Kernel.send(state.active_user, {:closed, self()})
    {:noreply, %{state | socket: nil}}
  end
end
