defmodule ClickhouseEcto.Connection do
  @behaviour Ecto.Adapters.SQL.Connection

  alias Clickhousex.Query
  alias ClickhouseEcto.Query, as: SQL

  @typedoc "The prepared query which is an SQL command"
  @type prepared :: String.t()

  @typedoc "The cache query which is a DBConnection Query"
  @type cached :: map

  @doc """
  Receives options and returns `DBConnection` supervisor child specification.
  """
  @impl Ecto.Adapters.SQL.Connection
  def child_spec(opts) do
    DBConnection.child_spec(Clickhousex.Protocol, opts)
  end

  @doc """
  Prepares and executes the given query with `DBConnection`.
  """
  @impl Ecto.Adapters.SQL.Connection
  @spec prepare_execute(
          connection :: DBConnection.t(),
          name :: String.t(),
          prepared,
          params :: [term],
          options :: Keyword.t()
        ) ::
          {:ok, query :: map, term} | {:error, Exception.t()}
  def prepare_execute(conn, name, prepared_query, params, options) do
    query = %Query{name: name, statement: prepared_query}

    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, query, result} ->
        {:ok, %{query | statement: prepared_query}, process_rows(result, options)}

      {:error, %Clickhousex.Error{}} = error ->
        if is_no_data_found_bug?(error, prepared_query) do
          {:ok, %Query{name: "", statement: prepared_query}, %{num_rows: 0, rows: []}}
        else
          error
        end

      {:error, error} ->
        raise error
    end
  end

  @doc """
  Executes the given prepared query with `DBConnection`.
  """
  @impl Ecto.Adapters.SQL.Connection
  @spec execute(
          connection :: DBConnection.t(),
          prepared_query :: prepared,
          params :: [term],
          options :: Keyword.t()
        ) ::
          {:ok, term} | {:error, Exception.t()}
  def execute(conn, %Query{} = query, params, options) do
    # IO.inspect(4)

    # IO.inspect(query)

    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, _query, result} ->
        # IO.inspect(result, label: "result")
        # IO.inspect(process_rows(result, options), label: "processed")
        {:ok, process_rows(result, options)}

      {:error, %Clickhousex.Error{}} = error ->
        if is_no_data_found_bug?(error, query.statement) do
          {:ok, %{num_rows: 0, rows: []}}
        else
          error
        end

      {:error, error} ->
        raise error
    end
  end

  def execute(conn, statement, params, options) do
    execute(conn, %Query{name: "", statement: statement}, params, options)
  end

  @impl Ecto.Adapters.SQL.Connection
  def query(conn, statement, params, options) do
    execute(conn, %Query{name: "", statement: IO.iodata_to_binary(statement)}, params, options)
  end

  defp is_no_data_found_bug?({:error, error}, statement) do
    is_dml =
      statement
      |> IO.iodata_to_binary()
      |> (fn string ->
            String.starts_with?(string, "INSERT") || String.starts_with?(string, "DELETE") ||
              String.starts_with?(string, "UPDATE")
          end).()

    is_dml and error.message =~ "No SQL-driver information available."
  end

  defp process_rows(result, options) do
    decoder = options[:decode_mapper] || fn x -> x end

    Map.update!(result, :rows, fn row ->
      unless is_nil(row), do: Enum.map(row, decoder)
    end)
  end

  def to_constraints(_error), do: []

  @doc """
  Returns a stream that prepares and executes the given query with `DBConnection`.
  """
  @impl Ecto.Adapters.SQL.Connection
  @spec stream(
          connection :: DBConnection.conn(),
          prepared_query :: prepared,
          params :: [term],
          options :: Keyword.t()
        ) ::
          Enum.t()
  def stream(_conn, _prepared, _params, _options) do
    raise("not implemented")
  end

  ## Queries
  @impl Ecto.Adapters.SQL.Connection
  def all(query) do
    SQL.all(query)
  end

  @impl Ecto.Adapters.SQL.Connection
  def update_all(query, prefix \\ nil), do: SQL.update_all(query, prefix)

  @impl Ecto.Adapters.SQL.Connection
  def delete_all(query), do: SQL.delete_all(query)

  @impl Ecto.Adapters.SQL.Connection
  def insert(prefix, table, header, rows, on_conflict, returning, opts) do
    SQL.insert(prefix, table, header, rows, on_conflict, returning, opts)
  end

  @impl Ecto.Adapters.SQL.Connection
  def update(prefix, table, fields, filters, returning) do
    SQL.update(prefix, table, fields, filters, returning)
  end

  @impl Ecto.Adapters.SQL.Connection
  def delete(prefix, table, filters, returning) do
    SQL.delete(prefix, table, filters, returning)
  end

  @impl Ecto.Adapters.SQL.Connection
  def execute_ddl(command), do: ClickhouseEcto.Migration.execute_ddl(command)

  @impl Ecto.Adapters.SQL.Connection
  def ddl_logs(%Clickhousex.Result{} = _result) do
    [{:info, "Some info message", []}]
  end

  @impl Ecto.Adapters.SQL.Connection
  def explain_query(_, _, _, _), do: raise("Not implemented")

  @impl Ecto.Adapters.SQL.Connection
  def table_exists_query(_), do: raise("Not implemented")

  @impl Ecto.Adapters.SQL.Connection
  def to_constraints(exception, options) do
    IO.inspect(exception, label: "exception")
    IO.inspect(options, label: "options")
    raise "Not implemented"
  end
end
