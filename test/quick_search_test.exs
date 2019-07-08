defmodule QuickSearchTest do
  use ExUnit.Case

  @moduletag :quick_search

  doctest FootbalEngine.QuickSearch

  alias FootbalEngine.QuickSearch

  describe "new" do
    @describetag :new

    test "creates keys according to data" do
      test_pid = self()
      deps = [
        storage_put: fn(key, val) ->
          send(test_pid, {:put, key, val})
          :ok
        end
      ]

      {:ok, :indexation_successful} =
        QuickSearch.new("./test/data/sample_1.csv", deps)

      expected_div = MapSet.new([
        %{
          "Div"       => "SP1",
          "Season"    => 201_718,
          "Date"      => ~D[2016-08-21],
          "HomeTeam"  => "Ath Madrid",
          "AwayTeam"  => "Alaves",
          "FTHG"      => 1,
          "FTAG"      => 1,
          "FTR"       => "D",
          "HTHG"      => 0,
          "HTAG"      => 0,
          "HTR"       => "D"
        },
        %{
          "Div"       => "SP1",
          "Season"    => 201_617,
          "Date"      => ~D[2016-08-19],
          "HomeTeam"  => "La Coruna",
          "AwayTeam"  => "Eibar",
          "FTHG"      => 2,
          "FTAG"      => 1,
          "FTR"       => "H",
          "HTHG"      => 0,
          "HTAG"      => 0,
          "HTR"       => "D"
        }
      ])

      expected_season = MapSet.new([
        %{
          "Div"       => "SP1",
          "Season"    => 201_617,
          "Date"      => ~D[2016-08-19],
          "HomeTeam"  => "La Coruna",
          "AwayTeam"  => "Eibar",
          "FTHG"      => 2,
          "FTAG"      => 1,
          "FTR"       => "H",
          "HTHG"      => 0,
          "HTAG"      => 0,
          "HTR"       => "D"
        },
        %{
          "Div"       => "SP2",
          "Season"    => 201_617,
          "Date"      => ~D[2016-08-19],
          "HomeTeam"  => "Malaga",
          "AwayTeam"  => "Osasuna",
          "FTHG"      => 1,
          "FTAG"      => 1,
          "FTR"       => "D",
          "HTHG"      => 0,
          "HTAG"      => 0,
          "HTR"       => "D"
        }
      ])

      expected_date = MapSet.new([
        %{
          "Div"       => "SP1",
          "Season"    => 201_617,
          "Date"      => ~D[2016-08-19],
          "HomeTeam"  => "La Coruna",
          "AwayTeam"  => "Eibar",
          "FTHG"      => 2,
          "FTAG"      => 1,
          "FTR"       => "H",
          "HTHG"      => 0,
          "HTAG"      => 0,
          "HTR"       => "D"
        },
        %{
          "Div"       => "SP2",
          "Season"    => 201_617,
          "Date"      => ~D[2016-08-19],
          "HomeTeam"  => "Malaga",
          "AwayTeam"  => "Osasuna",
          "FTHG"      => 1,
          "FTAG"      => 1,
          "FTR"       => "D",
          "HTHG"      => 0,
          "HTAG"      => 0,
          "HTR"       => "D"
        }
      ])

      expected_home_team = MapSet.new([
        %{
          "Div"       => "E0",
          "Season"    => 201_718,
          "Date"      => ~D[2016-08-21],
          "HomeTeam"  => "La Coruna",
          "AwayTeam"  => "Ath Bilbao",
          "FTHG"      => 2,
          "FTAG"      => 1,
          "FTR"       => "H",
          "HTHG"      => 0,
          "HTAG"      => 0,
          "HTR"       => "D"
        },
        %{
          "Div"       => "SP1",
          "Season"    => 201_617,
          "Date"      => ~D[2016-08-19],
          "HomeTeam"  => "La Coruna",
          "AwayTeam"  => "Eibar",
          "FTHG"      => 2,
          "FTAG"      => 1,
          "FTR"       => "H",
          "HTHG"      => 0,
          "HTAG"      => 0,
          "HTR"       => "D"
        }
      ])

      assert_receive {:put, {"Div", "SP1"},             ^expected_div       }
      assert_receive {:put, {"Season", "201617"},       ^expected_season    }
      assert_receive {:put, {"Date", "2016-08-19"},     ^expected_date      }
      assert_receive {:put, {"HomeTeam", "La Coruna"},  ^expected_home_team }
    end

    test "retuns partial success if at least one row in the CSV is malformed" do
      deps = [
        storage_put: fn(_key, _val) -> :ok end
      ]
      {:ok, :partial_indexation_successful, fails} =
        QuickSearch.new("./test/data/sample_2.csv", deps)

      expected_error = {:error, "Row has length 10 - expected length 12 on line 3"}
      assert Enum.member?(fails, expected_error)
    end

    test "retuns partial success if at least one row in the CSV cannot be parsed" do
      deps = [
        storage_put: fn(_key, _val) -> :ok end
      ]
      {:ok, :partial_indexation_successful, fails} =
        QuickSearch.new("./test/data/sample_3.csv", deps)

      expected_fails =[
        {:error, :unable_to_parse_int, "QGA2017"},
        {:error, :date_with_bad_format, "BAD_DATE"},
        {:error, :invalid_date, "99/99/2016"},
        {:error, :unable_to_parse_int, "AA"}
      ]

      assert fails == expected_fails
    end

    test "retuns error if it cannot read file" do
      {:error, reason} =
        QuickSearch.new("./test/data/non_existent_sample.csv")

      expected_reason = %File.Error{
        action: "stream",
        path: "./test/data/non_existent_sample.csv",
        reason: :enoent
      }

      assert Map.equal?(reason, expected_reason)
    end

    test "returns error if file has no data" do
      {:error, :no_valid_data_to_save, errors} =
        QuickSearch.new("./test/data/sample_4.csv")

      expected_errors = []

      assert errors === expected_errors
    end

    test "returns error if file is empty" do
      {:error, :no_valid_data_to_save, errors} =
        QuickSearch.new("./test/data/sample_5.csv")

      expected_errors = []

      assert errors === expected_errors
    end
  end

  describe "search" do
    @describetag :search

    test "returns all rows of a given header" do
      entry_1 = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_2 = %{
        "Div"       => "SP1",
        "Season"    => 201_819,
        "Date"      => ~D[2016-08-19],
        "HomeTeam"  => "La Coruna",
        "AwayTeam"  => "Eibar",
        "FTHG"      => 2,
        "FTAG"      => 1,
        "FTR"       => "H",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_3 = %{
        "Div"       => "SP2",
        "Season"    => 201_516,
        "Date"      => ~D[2016-08-20],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Sociedad",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_4 = %{
        "Div"       => "E0",
        "Season"    => 201_617,
        "Date"      => ~D[2016-08-19],
        "HomeTeam"  => "La Coruna",
        "AwayTeam"  => "Eibar",
        "FTHG"      => 2,
        "FTAG"      => 1,
        "FTR"       => "H",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      div_sp1 = MapSet.new([entry_1, entry_2])
      div_sp2 = MapSet.new([entry_3])
      season_201617 = MapSet.new([entry_4])

      test_data = [
        {{"Div", "SP1"}, div_sp1},
        {{"Div", "SP2"}, div_sp2},
        {{"Season", "201617"}, season_201617}
      ]

      deps = [
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = QuickSearch.search([{"Div", []}], deps)
      expected  = [entry_1, entry_2, entry_3]

      assert actual == expected
    end

    test "returns error when header does not exist" do
      entry = %{
        "Div"       => "SP2",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      test_data = [
        {{"Div", "SP2"}, MapSet.new([entry])},
      ]

      deps = [
        storage_get_all: fn -> test_data end
      ]

      {:error, :invalid_headers, headers} = QuickSearch.search([{"Banana", []}], deps)
      expected_headers  = ["Banana"]

      assert headers == expected_headers
    end

    test "returns empty when key does not exist" do
      entry = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      test_data = [
        {{"Div", "SP1"}, MapSet.new([entry])},
      ]

      deps = [
        storage_get: fn (_key, default) -> default end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = QuickSearch.search([{"Div", ["SP2"]}], deps)
      expected  = []

      assert actual == expected
    end

    test "returns all rows belonging to given key" do
      entry = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      test_data = [
        {{"Div", "SP1"}, MapSet.new([entry])},
      ]

      deps = [
        storage_get: fn (_key, _default) -> MapSet.new([entry]) end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = QuickSearch.search([{"Div", ["SP1"]}], deps)
      expected  = [entry]

      assert actual == expected
    end

    test "returns all rows belonging to multiple keys" do
      entry_1 = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_2 = %{
        "Div"       => "E0",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-11],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "La Coruna",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      div_sp1 = MapSet.new([entry_1])
      div_e0 = MapSet.new([entry_2])

      test_data = [
        {{"Div", "SP1"}, div_sp1},
        {{"Div", "E0"}, div_e0}
      ]

      deps = [
        storage_get: fn
          ({"Div", "SP1"}, _default) -> div_sp1
          ({"Div", "E0"}, _default) -> div_e0
        end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = QuickSearch.search([{"Div", ["SP1", "E0"]}], deps)
      expected  = [entry_1, entry_2]

      assert actual == expected
    end

    test "returns rows for keys that exist" do
      entry_1 = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      div_sp1 = MapSet.new([entry_1])

      test_data = [
        {{"Div", "SP1"}, div_sp1}
      ]

      deps = [
        storage_get: fn
          ({"Div", "SP1"}, _default)  -> div_sp1
          ({"Div", _}, _default)      -> MapSet.new()
        end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = QuickSearch.search([{"Div", ["SP1", "SP99999"]}], deps)
      expected  = [entry_1]

      assert actual == expected
    end

    test "returns commun rows for sets from different headers" do
      entry_1 = %{
        "Div"       => "SP1",
        "Season"    => 201_617,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_2 = %{
        "Div"       => "SP1",
        "Season"    => 201_819,
        "Date"      => ~D[2016-08-19],
        "HomeTeam"  => "La Coruna",
        "AwayTeam"  => "Eibar",
        "FTHG"      => 2,
        "FTAG"      => 1,
        "FTR"       => "H",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_3 = %{
        "Div"       => "E0",
        "Season"    => 201_617,
        "Date"      => ~D[2016-08-19],
        "HomeTeam"  => "La Coruna",
        "AwayTeam"  => "Eibar",
        "FTHG"      => 2,
        "FTAG"      => 1,
        "FTR"       => "H",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      div_sp1 = MapSet.new([entry_1, entry_2])
      season_201617 = MapSet.new([entry_3, entry_1])

      test_data = [
        {{"Div", "SP1"}, div_sp1},
        {{"Season", "201617"}, season_201617}
      ]

      deps = [
        storage_get: fn
          ({"Div", "SP1"}, _default)        -> div_sp1
          ({"Season", "201617"}, _default)  -> season_201617
        end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = QuickSearch.search(
        [
          {"Div",     ["SP1"]},
          {"Season",  ["201617"]}
        ],
        deps
      )
      expected  = [entry_1]

      assert actual == expected
    end
  end

end
