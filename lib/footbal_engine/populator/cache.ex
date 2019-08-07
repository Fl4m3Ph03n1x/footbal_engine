defmodule FootbalEngine.Populator.Cache do
  @moduledoc """
  Reads the CSV file, validates and parses its data and then populates the
  memory tables (the DB) with it's information.
  """

  alias FootbalEngine.Populator.{Parser, Storage}

  @default_deps [
    storage_get_all:  &:persistent_term.get/0,
    storage_get:      &:persistent_term.get/2,
    storage_put:      &:persistent_term.put/2,
    read_file:        &File.stream!/1
  ]

  @type index_status ::
    {:ok, :indexation_successful}
    | {:ok, :partial_indexation_successful, [any]}
    | {:error, :no_valid_data_to_save}
    | {:error, %{:__exception__ => true, :__struct__ => atom, optional(atom) => any}}

  @spec populate(String.t, keyword) :: index_status
  def populate(path, injected_deps \\ []) when is_binary(path) do
    deps = Keyword.merge(@default_deps, injected_deps)

    path
    |> read_file(deps)
    |> process_file_stream()
    |> validate_data()
    |> Storage.persist(deps)

  rescue
    err -> {:error, err}
  end

  ###############
  # Aux Functs  #
  ###############

  @spec read_file(String.t, keyword) :: Stream.t
  defp read_file(path, deps), do:
    path
    |> deps[:read_file].()
    |> CSV.decode(strip_fields: true, headers: true)

  @spec process_file_stream(Stream.t) :: {[map], [any]}
  defp process_file_stream(stream) do
    stream_data =
      stream
      |> Stream.filter(&by_stream_success/1)
      |> Stream.map(&extract_stream_data/1)
      |> Enum.map(&Parser.parse_data/1)

    stream_errors = Enum.filter(stream, &by_stream_error/1)

    {stream_data, stream_errors}
  end

  @spec validate_data({[map], [any]}) :: {[map], [any]}
  defp validate_data({stream_data, stream_errors}) do
    valid_data =
      stream_data
      |> Stream.filter(&by_parsed_data_success/1)
      |> Enum.map(&extract_parsed_data/1)

    invalid_data =
      stream_data
      |> Stream.filter(&by_parsed_data_error/1)
      |> Enum.concat(stream_errors)

    {valid_data, invalid_data}
  end

  @spec by_stream_error({:error, any} | any) :: boolean
  defp by_stream_error({:error, _data}), do: true
  defp by_stream_error(_entry), do: false

  @spec by_stream_success({:ok, any} | any) :: boolean
  defp by_stream_success({:ok, _data}), do: true
  defp by_stream_success(_entry), do: false

  @spec extract_stream_data({atom, map}) :: map
  defp extract_stream_data({_status, map}), do: map

  @spec by_parsed_data_success({:ok, any} | any) :: boolean
  defp by_parsed_data_success({:ok, _data}), do: true
  defp by_parsed_data_success(_entry), do: false

  @spec by_parsed_data_error({:error, atom, any} | any) :: boolean
  defp by_parsed_data_error({:error, _reason, _val}), do: true
  defp by_parsed_data_error(_entry), do: false

  @spec extract_parsed_data({:ok, map}) :: map
  defp extract_parsed_data({:ok, data}), do: data

end
