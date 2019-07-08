defmodule FootbalEngine.QuickSearch do
  @moduledoc """
  Database engine of the application. It can read the CSV file and index its
  contents for faster query searches. It can also query the index data.

  This module simulates an indexed (basic) memory db, where all the data is
  placed into memory and replicated for faster query searches. Memory usage will
  be inversionally proportional to query search times, meaning the more memory
  this solution uses, the faster the response times.
  """

  @default_deps [
    storage_get_all:  &:persistent_term.get/0,
    storage_get:      &:persistent_term.get/2,
    storage_put:      &:persistent_term.put/2,
    read_file:        &File.stream!/1
  ]

  ###############
  # Public API  #
  ###############

  @spec new(String.t, keyword) ::
    {:ok, :indexation_successful}
    | {:ok, :partial_indexation_successful, [any]}
    | {:error, :no_valid_data_to_save | any}
  def new(path, injected_deps \\ []) when is_binary(path) do
    deps = Keyword.merge(@default_deps, injected_deps)
    stream  =
      path
      |> deps[:read_file].()
      |> CSV.decode(strip_fields: true, headers: true)

    stream_data =
      stream
      |> Stream.filter(&by_stream_success/1)
      |> Stream.map(&extract_stream_data/1)
      |> Enum.map(&parse_data/1)

    stream_errors = Enum.filter(stream, &by_stream_error/1)

    errors =
      stream_data
      |> Stream.filter(&by_parsed_data_error/1)
      |> Enum.concat(stream_errors)

    valid_data =
      stream_data
      |> Stream.filter(&by_parsed_data_success/1)
      |> Enum.map(&extract_parsed_data/1)

    {save_status, save_msg} = store_data(valid_data, deps)

    cond do
      Enum.empty?(errors) and save_status == :ok ->
        {:ok, :indexation_successful}

      not(Enum.empty?(errors)) and save_status == :ok ->
        {:ok, :partial_indexation_successful, errors}

      true ->
        {:error, save_msg, errors}
    end

  rescue
    err -> {:error, err}
  end

  @spec search([{String.t, [String.t]}], keyword) ::
    {:ok, [map]}
    | {:error, :invalid_headers, [String.t]}
  def search(query, injected_deps \\ []) when is_list(query) do
    deps = Keyword.merge(@default_deps, injected_deps)

    query
    |> validate_headers(deps)
    |> perform_query(query, deps)
  end

  ###############
  # Aux Functs  #
  ###############

  @spec by_stream_error({:error, any} | any) :: boolean
  defp by_stream_error({:error, _data}), do: true
  defp by_stream_error(_entry), do: false

  @spec by_stream_success({:ok, any} | any) :: boolean
  defp by_stream_success({:ok, _data}), do: true
  defp by_stream_success(_entry), do: false

  @spec extract_stream_data({atom, map}) :: map
  defp extract_stream_data({_status, map}), do: map

  @spec by_parsed_data_error({:error, atom, any} | any) :: boolean
  defp by_parsed_data_error({:error, _reason, _val}), do: true
  defp by_parsed_data_error(_entry), do: false

  @spec by_parsed_data_success({:ok, any} | any) :: boolean
  defp by_parsed_data_success({:ok, _data}), do: true
  defp by_parsed_data_success(_entry), do: false

  @spec extract_parsed_data({:ok, map}) :: map
  defp extract_parsed_data({:ok, data}), do: data

  @spec parse_data(map) :: {:ok, map} | {:error, atom, String.t}
  defp parse_data(entry) do
    with  {:ok, season} <- parse_integer(Map.get(entry, "Season")),
          {:ok, fthg} <- parse_integer(Map.get(entry, "FTHG")),
          {:ok, ftag} <- parse_integer(Map.get(entry, "FTAG")),
          {:ok, hthg} <- parse_integer(Map.get(entry, "HTHG")),
          {:ok, htag} <- parse_integer(Map.get(entry, "HTAG")),
          {:ok, date} <- parse_date(Map.get(entry, "Date"))
    do
      parsed_data =
        entry
        |> Map.delete("")
        |> Map.put("Season", season)
        |> Map.put("FTHG", fthg)
        |> Map.put("FTAG", ftag)
        |> Map.put("HTHG", hthg)
        |> Map.put("HTAG", htag)
        |> Map.put("Date", date)

      {:ok, parsed_data}
    end
  end

  @spec parse_integer(String.t) ::
    {:ok, integer}
    | {:error, :unable_to_parse_int, String.t}
  defp parse_integer(val) do
    case Integer.parse(val) do
      {num, _remainder} -> {:ok, num}
      _err              -> {:error, :unable_to_parse_int, val}
    end
  end

  @spec parse_date(String.t) :: {:ok, Date.t} | {:error, atom, String.t}
  defp parse_date((<< day::bytes-2, "/", month::bytes-2, "/", year::bytes-4>>) = date_str) do
    with  {:ok, year}   <- parse_integer(year),
          {:ok, month}  <- parse_integer(month),
          {:ok, day}    <- parse_integer(day),
          {:ok, date}   <- Date.new(year, month, day)
    do
      {:ok, date}
    else
      {:error, :invalid_date} -> {:error, :invalid_date, date_str}
      err -> err
    end
  end

  defp parse_date(bad_date), do: {:error, :date_with_bad_format, bad_date}

  @spec store_data(list, keyword) ::
    {:ok, :save_successful}
    | {:error, :no_valid_data_to_save}
  defp store_data([], _deps), do: {:error, :no_valid_data_to_save}

  defp store_data(parsed_data, deps) do
    headers =
      parsed_data
      |> hd
      |> Map.keys()

    parsed_data
    |> generate_all_keys(headers)
    |> Enum.each(fn {key, val} -> deps[:storage_put].(key, val) end)

    {:ok, :save_successful}
  end

  @spec generate_all_keys([map], [String.t]) ::
    [{{String.t, String.t}, MapSet.t}]
  defp generate_all_keys(data, headers), do:
    Enum.flat_map(headers, &generate_header_keys(data, &1))

  @spec generate_header_keys([map], String.t) ::
    [{{String.t, String.t}, MapSet.t}]
  defp generate_header_keys(data, header) do
    data
    |> Stream.map(&get_header_values(&1, header))
    |> Stream.uniq()
    |> Enum.map(&create_index(&1, header, data))
  end

  @spec get_header_values(map, String.t) :: String.t
  defp get_header_values(entry, header), do: Map.get(entry, header)

  @spec create_index(String.t, String.t, [map]) ::
    {{String.t, String.t}, MapSet.t}
  defp create_index(curr_val, header, data) do
    matches =
      data
      |> Enum.filter(fn entry -> Map.get(entry, header) == curr_val end)
      |> MapSet.new()

    key = {header, to_string(curr_val)}
    {key, matches}
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

  @spec perform_query({:ok, :headers_valid} | any, [{String.t, [String.t] | []}], keyword) ::
    {:ok, [map]}
    | any
  defp perform_query({:ok, :headers_valid}, query, deps) do
    result =
      query
      |> Enum.map(&get_rows(&1, deps))
      |> multi_intersect()
      |> Enum.to_list()

    {:ok, result}
  end

  defp perform_query(err, _query, _deps), do: err
end
