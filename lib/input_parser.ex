defmodule MapReduce.InputParser do

  alias MapReduce.{
    Partition,
    InputReader
  }

  def main(args) do
    args |> parse_args |> start_map_reduce
  end

  defp parse_args(args) do
    {options, _, _} = args |> OptionParser.parse(switches: [file: :string])
    options
  end

  defp start_map_reduce([]), do: IO.puts :stderr, "NO FILE GIVEN"

  defp start_map_reduce(options) do
    partition_pid = elem(Partition.start_link, 1)
    InputReader.read("#{options[:file]}", partition_pid)
    keep_alive()
  end

  defp keep_alive do
    keep_alive()
  end

end
