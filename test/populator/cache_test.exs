defmodule FootbalEngine.Populator.GodzillaTest do
  use ExUnit.Case

  alias FootbalEngine.Populator.Cache, as: Populator

  @moduletag :populator

  describe "populate" do
    @describetag :populate

    test "creates keys according to data" do
      test_pid = self()
      deps = [
        storage_put: fn(key, val) ->
          send(test_pid, {:put, key, val})
          :ok
        end
      ]

      {:ok, :indexation_successful} =
        Populator.populate("./test/data/sample_1.csv", deps)

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
        Populator.populate("./test/data/sample_2.csv", deps)

      expected_error = {:error, "Row has length 10 - expected length 12 on line 3"}
      assert Enum.member?(fails, expected_error)
    end

    test "retuns partial success if at least one row in the CSV cannot be parsed" do
      deps = [
        storage_put: fn(_key, _val) -> :ok end
      ]
      {:ok, :partial_indexation_successful, fails} =
        Populator.populate("./test/data/sample_3.csv", deps)

      expected_fails = [
        {:error, :unable_to_parse_int, "QGA2017"},
        {:error, :date_with_bad_format, "BAD_DATE"},
        {:error, :invalid_date, "99/99/2016"},
        {:error, :unable_to_parse_int, "AA"}
      ]

      assert fails == expected_fails
    end

    test "retuns error if it cannot read file" do
      {:error, reason} =
        Populator.populate("./test/data/non_existent_sample.csv")

      expected_reason = %File.Error{
        action: "stream",
        path: "./test/data/non_existent_sample.csv",
        reason: :enoent
      }

      assert Map.equal?(reason, expected_reason)
    end

    test "returns error if file has no data" do
      {:error, :no_valid_data_to_save, errors} =
        Populator.populate("./test/data/sample_4.csv")

      expected_errors = []

      assert errors === expected_errors
    end

    test "returns error if file is empty" do
      {:error, :no_valid_data_to_save, errors} =
        Populator.populate("./test/data/sample_5.csv")

      expected_errors = []

      assert errors === expected_errors
    end
  end

end
