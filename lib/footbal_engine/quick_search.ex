defmodule FootbalEngine.QuickSearch do
  @moduledoc """
  Engine.
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

    errors = Enum.filter(stream, fn {status, _data} -> status == :error end)

    data =
      stream
      |> Stream.filter(fn {status, _data} -> status == :ok end)
      |> Stream.map(fn {_status, map} -> map end)
      |> Enum.map(&parse_data/1)

    errors = errors ++ Enum.filter(data, fn
      {:error, _reason, _val} -> true
      _ -> false
    end)

    parsed_data =
      data
      |> Stream.filter(fn
        {:ok, _data} -> true
        _ -> false
      end)
      |> Enum.map(fn {:ok, data} -> data end)

    {save_status, save_msg} = store_data(parsed_data, deps)

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

    with {:ok, :headers_valid} <- validate_headers(query, deps) do
      result =
        query
        |> Enum.map(&get_rows(&1, deps))
        |> multi_intersect()
        |> Enum.to_list()

      {:ok, result}
    end

  end

  ###############
  # Aux Functs  #
  ###############

  defp store_data([], _deps), do: {:error, :no_valid_data_to_save}

  defp store_data(parsed_data, deps) do
    headers =
      parsed_data
      |> hd
      |> Map.keys()

    parsed_data
    |> generate_all_keys(headers)
    |> Enum.each(fn {key, val} ->
      deps[:storage_put].(key, val)
    end)

    {:ok, :save_successful}
  end

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

  defp parse_integer(val) do
    case Integer.parse(val) do
      {num, _remainder} -> {:ok, num}
      _err              -> {:error, :unable_to_parse_int, val}
    end
  end

  defp parse_date(<< day::bytes-2, "/", month::bytes-2, "/", year::bytes-4>>) do

    with  {:ok, year} <- parse_integer(year),
          {:ok, month} <- parse_integer(month),
          {:ok, day} <- parse_integer(day)
    do
      Date.new(year, month, day)
    end
  end

  defp generate_all_keys(data, headers), do:
    Enum.flat_map(headers, &generate_header_keys(data, &1))

  defp generate_header_keys(data, header) do
    data
    |> Stream.map(&get_header_values(&1, header))
    |> Stream.uniq()
    |> Enum.map(&create_index(&1, header, data))
  end

  defp get_header_values(entry, header), do: Map.get(entry, header)

  defp create_index(curr_val, header, data) do
    matches =
      data
      |> Enum.filter(fn entry -> Map.get(entry, header) == curr_val end)
      |> MapSet.new()

    key = {header, to_string(curr_val)}
    {key, matches}
  end

  defp multi_intersect(sets) do
    Enum.reduce(
      sets,
      hd(sets),
      fn (set, acc) -> MapSet.intersection(acc, set) end
    )
  end

  defp multi_union(sets) do
    Enum.reduce(
      sets,
      hd(sets),
      fn (set, acc) -> MapSet.union(acc, set) end
    )
  end

  defp validate_headers(query, deps) do
    valid_headers =
      deps[:storage_get_all].()
      |> Stream.filter(fn
        {{header, val}, _set} when is_binary(header) and is_binary(val) -> true
        _ -> false
      end)
      |> Enum.map(fn {{header, _val}, _set} -> header end)
      |> MapSet.new()

    query_headers =
      query
      |> Enum.map(fn {header, _vals} -> header end)
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

end
