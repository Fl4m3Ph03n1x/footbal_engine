defmodule FootbalEngine.Populator.Server do
  @moduledoc """
  Server for the Cache. Tries to populate it with data and if it gets anything
  other than a complete success for the indexation, it will keep trying to
  repopulate the memory tables.
  """

  use GenServer

  alias FootbalEngine.Populator.Cache

  ###############
  # Public API  #
  ###############

  @spec start_link(String.t) :: GenServer.on_start
  def start_link(path), do:
    GenServer.start_link(__MODULE__, path)

  ###############
  #  Callbacks  #
  ###############

  @impl GenServer
  @spec init(String.t) :: {:ok, String.t} | {:stop, any}
  def init(file_path) do
    :persistent_term.put(:indexation_status, :initializing)
    check_file_with_msg(file_path, {:ok, file_path})
  end

  @impl GenServer
  def handle_info({:check_status}, file_path), do:
    check_file_with_msg(file_path, {:noreply, file_path})

  ###############
  # Aux Functs  #
  ###############

  @spec check_file_with_msg(String.t, any) :: any
  defp check_file_with_msg(file_path, msg) do

    case Cache.populate(file_path) do
      status = {:ok, :indexation_successful} ->
        :persistent_term.put(:indexation_status, status)

      bad_status ->
        :persistent_term.put(:indexation_status, bad_status)
        {:ok, _ref} = :timer.send_after(15_000, {:check_status})
    end

    msg
  end

end
