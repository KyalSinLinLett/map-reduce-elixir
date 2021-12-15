defmodule MapReduce.OutputWriter do
  def start_link do
    Task.start_link(fn -> loop([], []) end)
  end

  defp loop(values, processes) do
    reducer_proc_mailbox_len = Process.info(self(), :message_queue_len) |> elem(1)
    if reducer_proc_mailbox_len == 0, do:
      prep_reducer(processes, values)
    receive do
      {:reduce_key_put, caller} -> loop(values, [caller | processes])
      {:reduce_value_put, value} -> loop([value | values], processes)
    end
  end

  defp prep_reducer(processes, values) do
    alive_reduce_processes = Enum.filter(processes, fn proc -> Process.alive?(proc) == true end)

    if length(alive_reduce_processes) == 0 and length(values) > 0 do
      {:ok, out_file} = File.open(Path.join("test", "output.txt"), [:write])
      for value <- values do
        IO.write(out_file, value <> "\n")
      end
      File.close(out_file)
      Process.exit(self(), :kill)
    end
  end
end
