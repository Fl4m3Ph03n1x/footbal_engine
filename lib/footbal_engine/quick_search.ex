defmodule QuickSearch do
  use GenServer

  @default_deps %{
    tables: %{
      division: :division,
      season: :season
    }
  }

  ###############
  # Public API  #
  ###############

  def division(:all, deps \\ @default_deps), do:
    :ets.tab2list(deps.tables.division)


  def division(divisions, deps \\ @default_deps) when is_list(divisions) do
    :ets.lookup(arg1, arg2)
  end

  ###############
  # Aux Functs  #
  ###############
end
