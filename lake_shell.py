import cmd
import requests
import pandas as pd
import io
import shlex
import csv
import sys
from rich.console import Console
from rich.table import Table

console = Console()

API_URL = "http://localhost:8000"

class LakeShell(cmd.Cmd):
    intro = '[bold cyan]🌊 Welcome to the Lakehouse Data Shell![/]\nType [green]help[/] or [green]?[/] to list commands.\n'
    prompt = '(lake) '

    def do_insert(self, arg):
        """
        Insert a new record.
        Usage: insert <table> <pkey_col> <val> <col1>=<val1> ...
        Example: insert users id 101 name="Alice" role="dev"
        """
        args = shlex.split(arg)
        if len(args) < 3:
            console.print("[red]❌ Usage: insert <table> <pkey_col> <val> <col1>=<val1> ...[/]")
            return

        table = args[0]
        pkey_col = args[1]
        pkey_val = args[2]
        kv_pairs = args[3:]

        row = {pkey_col: pkey_val}
        for pair in kv_pairs:
            if '=' not in pair:
                 console.print(f"[yellow]⚠️  Skipping invalid formatted argument: {pair}[/]")
                 continue
            k, v = pair.split('=', 1)
            row[k] = v

        # Convert to CSV
        df = pd.DataFrame([row])
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()

        self._ingest(table, csv_content, pkey_col)

    def do_update(self, arg):
        """
        Update an existing record (Thread-Safe Read-Modify-Write).
        Usage: update <table> <pkey_col> <val> <col>=<new_val> ...
        Example: update users id 101 role="manager"
        """
        args = shlex.split(arg)
        if len(args) < 4:
            console.print("[red]❌ Usage: update <table> <pkey_col> <val> <col>=<new_val> ...[/]")
            return

        table = args[0]
        pkey_col = args[1]
        pkey_val = args[2]
        updates = args[3:]
        
        console.print(f"[dim]🔍 Fetching current state of {pkey_col}={pkey_val} in {table}...[/]")
        
        # 1. READ
        try:
            resp = requests.get(
                f"{API_URL}/hudi/{table}/read", 
                params={"filter_col": pkey_col, "filter_val": pkey_val}
            )
            data = resp.json()
            if not data:
                console.print(f"[red]❌ Record not found![/]")
                return
            
            # Assume single record for ID
            record = data[0]
        except Exception as e:
            console.print(f"[red]❌ Error reading data: {e}[/]")
            return

        # 2. MATCH & PATCH
        changes = {}
        for u in updates:
            if '=' in u:
                k, v = u.split('=', 1)
                changes[k] = v
                record[k] = v # Apply update
        
        # 3. WRITE BACK
        df = pd.DataFrame([record])
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        console.print(f"[dim]📝 Applying updates: {changes}[/]")
        self._ingest(table, csv_content, pkey_col)

    def do_select(self, arg):
        """
        Read data from a table.
        Usage: select <table> [limit]
        Example: select users 5
        """
        args = shlex.split(arg)
        if not args:
             console.print("[red]❌ Usage: select <table> [limit][/]")
             return
             
        table = args[0]
        limit = args[1] if len(args) > 1 else "10"
        
        try:
            resp = requests.get(f"{API_URL}/hudi/{table}/read", params={"limit": limit})
            if resp.status_code == 200:
                data = resp.json()
                self._print_table(data)
            else:
                console.print(f"[red]❌ Failed: {resp.text}[/]")
        except Exception as e:
             console.print(f"[red]❌ Error: {e}[/]")

    def do_delete(self, arg):
        """
        Delete a record.
        Usage: delete <table> <pkey_col> <val>
        Example: delete users id 101
        """
        args = shlex.split(arg)
        if len(args) < 3:
             console.print("[red]❌ Usage: delete <table> <pkey_col> <val>[/]")
             return

        table = args[0]
        pkey = args[1]
        val = args[2]

        data_payload = {
            'table': table,
            'pkey': pkey,
            'ids': val
        }

        try:
            resp = requests.post(f"{API_URL}/delete/hudi", data=data_payload)
            if resp.status_code == 200:
                console.print(f"[green]✅ Deleted {pkey}={val} from {table}[/]")
            else:
                console.print(f"[red]❌ API Error: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/]")

    def do_get(self, arg):
        """
        Get a specific record.
        Usage: get <table> <pkey_col> <val>
        Example: get users id 101
        """
        args = shlex.split(arg)
        if len(args) < 3:
            console.print("[red]❌ Usage: get <table> <pkey_col> <val>[/]")
            return
            
        table = args[0]
        col = args[1]
        val = args[2]
        
        try:
            resp = requests.get(
                f"{API_URL}/hudi/{table}/read", 
                params={"filter_col": col, "filter_val": val}
            )
            if resp.status_code == 200:
                self._print_table(resp.json())
            else:
                console.print(f"[red]❌ Error: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Connection Error: {e}[/]")

    def do_sql(self, arg):
        """
        Run a raw SQL query via Trino.
        Usage: sql <query>
        Example: sql SELECT * FROM hudi.default.users
        """
        if not arg:
             console.print("[red]❌ Usage: sql <query>[/]")
             return
             
        try:
            # We assume the generic trino endpoint can handle this
            resp = requests.get(f"{API_URL}/query/trino", params={"sql": arg, "catalog": "hudi"})
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                     self._print_table(data)
                else:
                     console.print(data)
            else:
                console.print(f"[red]❌ Query Error: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Connection Error: {e}[/]")

    def _ingest(self, table, csv_content, pkey):
        files = {'file': ('shell_upload.csv', csv_content, 'text/csv')}
        data = {'table': table, 'pkey': pkey}
        
        try:
            console.print("[dim]🚀 Transmitting to Lakehouse...[/]")
            resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
            if resp.status_code == 200:
                console.print(f"[green]✅ Success![/]")
            else:
                 console.print(f"[red]❌ API Response: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Request Failed: {e}[/]")

    def _print_table(self, data):
        if not data:
            console.print("[yellow]⚠️  No results found.[/]")
            return
            
        # Dynamically create table based on first row keys
        first = data[0]
        headers = list(first.keys())
        
        t = Table(show_header=True, header_style="bold magenta")
        for h in headers:
            t.add_column(h)
            
        for row in data:
            vals = [str(row.get(h, "")) for h in headers]
            t.add_row(*vals)
            
        console.print(t)

    def do_exit(self, arg):
        """Exit the shell."""
        console.print("👋 Bye!")
        return True

if __name__ == '__main__':
    try:
        LakeShell().cmdloop()
    except KeyboardInterrupt:
        print("\nBye!")
