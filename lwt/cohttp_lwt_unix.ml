(*
 * Copyright (c) 2012 Anil Madhavapeddy <anil@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *)

let check_debug norm_fn debug_fn =
  try
    ignore(Sys.getenv "COHTTP_DEBUG");
    debug_fn
  with Not_found ->
    norm_fn
    
module IO = struct

  type 'a t = 'a Lwt.t
  let (>>=) = Lwt.bind
  let return = Lwt.return

  type ic = Lwt_io.input_channel
  type oc = Lwt_io.output_channel

  let iter fn x = Lwt_list.iter_s fn x

  let read_line =
    check_debug
      (fun ic -> Lwt_io.read_line_opt ic)
      (fun ic ->
        match_lwt Lwt_io.read_line_opt ic with
        |None as x -> Printf.eprintf "<<< EOF\n"; return x
        |Some l as x -> Printf.eprintf "<<< %s\n" l; return x)

  let read =
   check_debug
     (fun ic count ->
       try_lwt Lwt_io.read ~count ic
       with End_of_file -> return "")
     (fun ic count ->
       lwt buf = 
         try_lwt Lwt_io.read ~count ic
         with End_of_file -> return "" in
       Printf.eprintf "<<< %s" buf;
       return buf)

  let read_exactly =
    check_debug
      (fun ic buf off len ->
        try_lwt Lwt_io.read_into_exactly ic buf off len >> return true
        with End_of_file -> return false)
     (fun ic buf off len ->
        lwt rd =
          try_lwt Lwt_io.read_into_exactly ic buf off len >> return true
          with End_of_file -> return false in
        (match rd with
        |true -> Printf.eprintf "<<< %S" (String.sub buf off len)
        |false -> Printf.eprintf "<<< <EOF>\n");
        return rd)

  let write =
    check_debug
      (fun oc buf -> Lwt_io.write oc buf)
      (fun oc buf -> Printf.eprintf ">>> %s" buf; Lwt_io.write oc buf)

  let write_line =
    check_debug
      (fun oc buf -> Lwt_io.write_line oc buf)
      (fun oc buf -> Printf.eprintf ">>> %s\n" buf; Lwt_io.write_line oc buf)
end

module Request = Cohttp.Request.Make(IO)
module Response = Cohttp.Response.Make(IO)
module Body = Cohttp_lwt_body
module Net = Cohttp_lwt_net
module Client = Cohttp_lwt.Client(Request)(Response)(Net)
module Server = Cohttp_lwt.Server(Request)(Response)(Net)

let server ?timeout ~address ~port spec =
  lwt sockaddr = Net.build_sockaddr address port in
  Net.Tcp_server.init ~sockaddr ?timeout (Server.callback spec)
