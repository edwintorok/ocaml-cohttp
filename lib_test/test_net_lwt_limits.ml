open Lwt
open Cohttp

let print_outcome t0 name uri response =
  let dt = Unix.gettimeofday () -. t0 in
  Lwt_io.printlf "%s %s in %.3fs: %s" name (Uri.to_string uri) dt
    (Response.status response |> Code.string_of_status)

let print_error name uri exn =
  Lwt_io.eprintlf "Test %S failed on %s: %s" name (Uri.to_string uri) (Printexc.to_string exn)

let repeat ?(batch=64) n f arg =
  let rec loop n =
    if n < 1 then Lwt.fail_invalid_arg "repeat"
    else if n = 1 then f arg
    else f arg >>= fun _ -> loop (n-1)
  in
  Array.init batch (fun i ->
      n / batch + (if i = 0 then n mod batch else 0)) |> Array.to_list |>
  Lwt_list.map_p loop >|= List.hd

let test_connect_head uri =
  Cohttp_lwt_unix.Client.head uri

let test_leak uri =
  Cohttp_lwt_unix.Client.head uri

let test_idle uri =
  let s, push = Lwt_stream.create () in
  Cohttp_lwt_unix.Client.callv uri s >>= Lwt_stream.to_list >|= List.hd >|= fst

let perform uri =
  let run_test (name, f) =
    let t0 = Unix.gettimeofday () in
    Lwt.try_bind (fun () -> f uri)
      (print_outcome t0 name uri)
      (print_error name uri)
  in
  let t = List.rev_map run_test [
    "connect", test_connect_head;
    "leak", repeat 2048 test_leak;
    "idle", test_idle
    ] |> Lwt.join in
  let handler = Lwt_unix.on_signal Sys.sigint (fun _ -> Lwt.cancel t) in
  Lwt.on_termination t (fun () -> Lwt_unix.disable_signal_handler handler);
  t

let () =
  if Array.length Sys.argv <> 2 then begin
    Printf.eprintf "Usage: %s <target>\n" Sys.argv.(0);
    exit 1;
  end;
  Lwt_main.run (Lwt_unix.handle_unix_error perform (Uri.of_string Sys.argv.(1)))
