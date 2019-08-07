defmodule FootbalEngine.Populator do
  @moduledoc """
  Interface for the populator that fills up the memory table (populates it) with
  data.
  """

  alias FootbalEngine.Populator.Server

  @spec new(String.t) :: GenServer.on_start
  def new(path), do: Server.start_link(path)
end
