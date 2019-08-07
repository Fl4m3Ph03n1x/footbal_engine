defmodule FootbalEngine.Reader do
  @moduledoc """
  Client that queries the memory DB for information.

  To use this API the
  engine must have populated the memory table before, otherwise all operations
  will fail.
  """

  @default_deps [
    storage_get_all:  &:persistent_term.get/0,
    storage_get:      &:persistent_term.get/2,
    storage_put:      &:persistent_term.put/2,
    read_file:        &File.stream!/1
  ]

  @spec search([{String.t, [String.t]}], keyword) ::
    {:ok, [map]}
    | {:error, :invalid_headers, [String.t]}
    | {:error, :indexation_not_ready}
  def search(query, injected_deps \\ []) when is_list(query) do
    deps = Keyword.merge(@default_deps, injected_deps)
    status = deps[:storage_get].(:indexation_status, {:error, :engine_not_created})

    with  {:ok, :ready}         <- check_cache(status),
          {:ok, :headers_valid} <- validate_headers(query, deps)
    do
      perform_query(query, deps)
    end

  end

  ###############
  # Aux Functs  #
  ###############

  @spec check_cache(
    {:ok, :indexation_successful}
    | {:ok, :partial_indexation_successful, any}
    | :initializing
    | any
  ) :: {:ok, :ready} |  {:error, :indexation_not_ready} | any
  defp check_cache({:ok, :indexation_successful}), do: {:ok, :ready}
  defp check_cache({:ok, :partial_indexation_successful, _}), do: {:ok, :ready}
  defp check_cache(:initializing), do: {:error, :indexation_not_ready}
  defp check_cache(error), do: error

  @spec validate_headers([{String.t, [String.t]}], keyword) ::
    {:ok, :headers_valid}
    | {:error, :invalid_headers, [String.t]}
  defp validate_headers(query, deps) do
    valid_headers =
      deps[:storage_get_all].()
      |> Stream.filter(&by_valid_keys/1)
      |> Enum.map(&extract_key_header/1)
      |> MapSet.new()

    query_headers =
      query
      |> Enum.map(&extract_query_header/1)
      |> MapSet.new()

    invalid_headers =
      query_headers
      |> MapSet.difference(valid_headers)
      |> Enum.to_list()

    case invalid_headers do
      [] -> {:ok, :headers_valid}
      _  -> {:error, :invalid_headers, invalid_headers}
    end
  end

  @spec by_valid_keys({{String.t, String.t}, MapSet.t}) :: boolean
  defp by_valid_keys({{header, val}, _set})
    when is_binary(header) and is_binary(val), do: true

  defp by_valid_keys({_key, _val}), do: false

  @spec extract_key_header({{String.t, String.t}, MapSet.t}) :: String.t
  defp extract_key_header({{header, _val}, _set}), do: header

  @spec extract_query_header({String.t, [String.t]}) :: String.t
  defp extract_query_header({header, _val}), do: header

  @spec get_rows({String.t, [String.t] | []}, keyword) :: MapSet.t
  defp get_rows({header, []}, deps) do
    deps[:storage_get_all].()
    |> Stream.filter(fn
      {{^header, _val}, _set} -> true
      _ -> false
    end)
    |> Enum.map(fn {_key, val} -> val end)
    |> multi_union()
  end

  defp get_rows({header, vals}, deps), do:
    vals
    |> Enum.map(&deps[:storage_get].({header, &1}, MapSet.new()))
    |> multi_union()

  @spec perform_query([{String.t, [String.t] | []}], keyword) :: {:ok, [map]}
  defp perform_query(query, deps) do
    result =
      query
      |> Enum.map(&get_rows(&1, deps))
      |> multi_intersect()
      |> Enum.to_list()

    {:ok, result}
  end

  @spec multi_intersect([MapSet.t]) :: MapSet.t
  defp multi_intersect(sets), do:
    Enum.reduce(
      sets,
      hd(sets),
      fn (set, acc) -> MapSet.intersection(acc, set) end
    )

  @spec multi_union([MapSet.t]) :: MapSet.t
  defp multi_union(sets), do:
    Enum.reduce(
      sets,
      hd(sets),
      fn (set, acc) -> MapSet.union(acc, set) end
    )

end
