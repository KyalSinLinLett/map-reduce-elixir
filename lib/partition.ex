defmodule MapReduce.Partition do
  alias MapReduce.OutputWriter
  alias MapReduce.ReduceFunction, as: Reduce

  def start_link do
    Task.start_link(fn -> loop([], []) end)
  end

  defp loop(values, processes) do
    partition_proc_mailbox_len = Process.info(self(), :message_queue_len) |> elem(1)
    if partition_proc_mailbox_len == 0, do:
      prep_reducer(processes, Keyword.delete(values, String.to_atom(~s(\s))) |> Keyword.delete(String.to_atom("")))
    receive do
      {:map_key_put, caller} -> loop(values, [caller | processes])
      {:map_value_put, value} -> loop([{String.to_atom(value), 1} | values], processes)
    end
  end

  defp prep_reducer(processes, values) do
    alive_map_processes = Enum.filter(processes, fn proc -> Process.alive?(proc) == true end)
    unique_keys = values |> Keyword.keys() |> Enum.uniq()

    if length(alive_map_processes) == 0 and length(unique_keys) > 0 do
      output_writer_pid = OutputWriter.start_link |> elem(1)
      Enum.each(
        unique_keys,
        fn unique_key ->
          spawn(fn ->
            Reduce.reduce(Keyword.take(values, [unique_key]) |> Keyword.to_list(), output_writer_pid)
          end)
        end)
    end
  end
end
