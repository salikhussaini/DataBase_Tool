import tkinter as tk
import os
from tkinter import filedialog, messagebox, ttk    # Import messagebox from tkinter
from tkinter import filedialog
import sqlite3
import pandas as pd
import pyodbc

from teradataml.context.context import create_context,remove_context,get_connection
from teradataml.dataframe.dataframe import DataFrame
from teradataml import fastexport

class TeradataHandler:
    def __init__(self, root):
        self.root = root
        self.dir_path = os.path.dirname(os.path.abspath(__file__))
        # TeraData Inputs
        self.TD_DSN = 'teradata-data.fyiblue.com'
        self.TD_Driver = 'Teradata Database ODBC Driver 16.20'
        self.conn = None
        self.TD_UserName = None
        self.TD_PassWord = None

        # TeraData Output
        self.DBs = None
        self.tables = None
        self.table_definition = None

        # Teradata Selected Inputs
        self.selected_db = None
        self.selected_table = None
    def connect_td_ml(self):
        try:
            #Create a connection
            create_context(
                host=self.TD_DSN
                , username=self.TD_UserName
                , password=self.TD_PassWord
                , logmech='LDAP'
            )
            #Get Connection Object
            self.conn = get_connection()
        except:
            pass
    def disconnect_td_ml(self):
        try:
            remove_context()
        except Exception as e:
            print(f'Error Occured: {e}')
    def td_ml_select(self):
        # Connect to TeraData
        self.connect_td_ml()
        #Define Custom Q
        custom_query = f"SELECT * FROM {self.selected_db}.{self.selected_table}"
        #Execute custom SQL queries using the DataFrame.from_query() method
        self.td_dataframe = DataFrame.from_query(custom_query)
    def td_ml_export(self):
        file_path = filedialog.askdirectory()
        self.td_ml_select()
        #Export TeraDataML DataFrame to Pandas DataFrame
        self.pd_dataframe = self.td_dataframe.to_pandas()
        self.pd_dataframe.to_parquet(f'{file_path}/{self.selected_db}.{self.selected_table}.gzip',compression='gzip')
        status_bar_credentials.config(text=f"File Exported to {file_path}")
        self.disconnect_td_ml()
    def td_ml_export_2(self):
        file_path = filedialog.askdirectory()
        self.td_ml_select()
        #Export TeraDataML DataFrame to Pandas DataFrame
        pandas_df, err = fastexport(self.td_dataframe)
        pandas_df.to_parquet(f'{file_path}/{self.selected_db}.{self.selected_table}.gzip',compression='gzip')
        status_bar_credentials.config(text=f"File Exported to {file_path}")
        self.disconnect_td_ml()
    def export_table_widgets(self):        
        # Export DBs Button
        TD_8_Button = tk.Button(credentials_window, text="Export Table", command=self.td_ml_export_2)
        TD_8_Button.grid(row=14, column=0, columnspan=2, pady=10)
    def export_table_definition_widgets(self):        
        # Export DBs Button
        TD_8_Button = tk.Button(credentials_window, text="Export Table Definition", command=self.export_table_def)
        TD_8_Button.grid(row=13, column=0, columnspan=2, pady=10)
    def export_table_def(self):
        # Open a file dialog to select CSV file
        file_path = filedialog.askdirectory()
        if file_path:
            self.label.config(text=f"Selected File: {file_path}")
            file_path = f'{file_path}\\{self.selected_db}.{self.selected_table}.sql'
            with open(file_path,'w') as file:
                file.write(self.table_definition)

        self.export_table_widgets()
    def fetch_table_def(self):
        if self.conn is None:
            self.show_message("Warning", "Please connect to Teradata first.", "warning")
            return
        else:
            try:
                # Fetch and display table names
                cursor = self.conn.cursor()
                get_table_for_user = f"""
                    show TABLE {self.selected_db}.{self.selected_table}
                ;
                """
                print(get_table_for_user)
                cursor.execute(get_table_for_user)
                self.table_definition = [row[0].strip() for row in cursor.fetchall()][0]
                self.table_definition = self.table_definition.replace('\r     ','')
                # Close the cursor and connection
                cursor.close()

                self.export_table_definition_widgets()
            except pyodbc.Error as e:
                self.show_message("Error", f"Error connecting to Teradata: {e}", "warning")
    def get_table_definition_widgets(self):        
        # Export DBs Button
        TD_7_Button = tk.Button(credentials_window, text="Get Table Definition", command=self.get_table_definition)
        TD_7_Button.grid(row=12, column=0, columnspan=2, pady=10)
    def get_table_definition(self):
        self.fetch_table_def()
        if self.table_definition is not None:
            print(self.table_definition)
    def get_credentials_window(self):
        # Function to get credentials when OK button is pressed
        def get_credentials():
            self.TD_UserName = username_entry.get()
            self.TD_PassWord = password_entry.get()
            if self.TD_UserName == '' or self.TD_PassWord == '':
                self.show_message("Warning", "UserName or PassWord Not Entered", "warning")
                return
            else:
                status_bar_credentials.config(text="Credentials entered")             
        def connect_to_teradata():
            get_credentials()
            if ((self.TD_UserName != '' and self.TD_PassWord != '') or (self.TD_UserName is None and self.TD_PassWord is None)):
                try:
                    # Construct the ODBC connection string
                    odbc_connection_string = f"DRIVER={self.TD_Driver};DBCNAME={self.TD_DSN};UID={self.TD_UserName};PWD={self.TD_PassWord};authentication=LDAP"
                    # Establish ODBC connection to Teradata
                    conn = pyodbc.connect(odbc_connection_string)
                    status_bar_credentials.config(text="Connected to Teradata")
                    self.conn = conn
                except pyodbc.Error as e:
                    status_bar_credentials.config(text=f"Error connecting to Teradata: {e}")
                    return None
        def fetch_teradata_DBs():
            if self.conn is None:
                self.show_message("Warning", "Please connect to Teradata first.", "warning")
                return
            try:
                # Fetch and display table names
                cursor = self.conn.cursor()
                get_table_for_user = f"""
                    select distinct T2.DatabaseName
                    from (select RoleName from DBC.ROLEMEMBERS where Grantee = '{self.TD_UserName}') T1
                    inner join (
                        SELECT distinct RoleName,	DatabaseName FROM DBC.ALLROLERIGHTS
                        ) T2
                    on T1.RoleName = T2.RoleName
                
                """
                cursor.execute(get_table_for_user)
                self.DBs = [row[0].strip() for row in cursor.fetchall()]
                # Close the cursor and connection
                cursor.close()
            except pyodbc.Error as e:
                self.show_message("Error", f"Error connecting to Teradata: {e}", "warning")
        def show_teradata_DBs():
            if self.DBs is None:
                fetch_teradata_DBs()
            # Create a new window for displaying Teradata tables
            tables_window = tk.Toplevel(self.root)
            tables_window.title("Teradata Tables")

            # Create a Text widget for displaying table names
            tables_text = tk.Text(tables_window, wrap=tk.WORD)
            tables_text.pack()

            # Insert table names into the Text widget
            for table in self.DBs:
                tables_text.insert(tk.END, f"{table}\n")
        def export_teradata_DBs():
            if self.DBs is None:
                fetch_teradata_DBs()
            else:
                file_path = f'{self.dir_path}\\Data\\User_Access_DataBase.csv'
                with open(file_path,'w') as file:
                    # Insert table names into the Text widget
                    file.write(f"Table_Number,DataBase\n")
                    for idx,table in enumerate(self.DBs, start=1):
                        file.write(f"{idx},{table}\n")
                status_bar_credentials.config(text=f"DataBase access file exported to:\n{file_path}")      
        def fetch_teradata_Tables():
            if self.conn is None:
                self.show_message("Warning", "Please connect to Teradata first.", "warning")
                return
            try:
                # Fetch and display table names
                cursor = self.conn.cursor()
                get_table_for_user = f"""
                    select distinct T3.DatabaseName, T3.TableName
                    from (
                        select RoleName from DBC.ROLEMEMBERS where Grantee = '{self.TD_UserName}'
                    ) T1
                    inner join (
                        SELECT distinct RoleName, DatabaseName FROM DBC.ALLROLERIGHTS
                        ) T2
                    on T1.RoleName = T2.RoleName
                    inner join (
                        SELECT distinct DatabaseName, TableName FROM DBC.TablesX
                        where TableKind = 'T'
                        ) T3
                    on T2.DatabaseName = T3.DatabaseName
                    order by T3.DatabaseName, T3.TableName
                    ;
                """
                cursor.execute(get_table_for_user)
                self.tables = [[row[0].strip(),row[1].strip()] for row in cursor.fetchall()]
                # Close the cursor and connection
                cursor.close()
            except pyodbc.Error as e:
                self.show_message("Error", f"Error connecting to Teradata: {e}", "warning")
        def show_teradata_Tables():
            if self.tables is None:
                fetch_teradata_Tables()
            # Create a new window for displaying Teradata tables
            tables_window = tk.Toplevel(self.root)
            tables_window.title("Teradata Tables")

            # Create a Text widget for displaying table names
            tables_text = tk.Text(tables_window, wrap=tk.WORD)
            tables_text.pack()

            # Insert table names into the Text widget
            for table in self.tables:
                tables_text.insert(tk.END, f"{table[0]},{table[1]}\n")
        def export_teradata_Tables():
            if self.tables is None:
                fetch_teradata_Tables()
            else:
                file_path = f'{self.dir_path}\\Data\\User_Access_Tables.csv'
                with open(file_path,'w') as file:
                    # Insert table names into the Text widget
                    file.write(f"Table_Number,DataBase,Table_Name\n")
                    for idx,table in enumerate(self.tables, start=1):
                        file.write(f"{idx},{table[0]},{table[1]}\n")
                status_bar_credentials.config(text=f"Tables access file exported to:\n{file_path}")
            
        def on_select_db():
            self.selected_db = db_list_widget.get()
            create_tables_select()
        def on_select_table():
            self.selected_table = tables_list_widget.get()
            self.get_table_definition_widgets()

        def create_db_select(db_list):
            # Create ComboBox Widget 
            
            global db_list_widget
            
            db_list_widget = ttk.Combobox(credentials_window, values = db_list)
            db_list_widget.grid(row=10, column=0, columnspan=1, pady=10)
            # Set a Default Value
            db_list_widget.set('Select a DB')

            # Bind the even 
            TD_1_button = tk.Button(credentials_window, text="GET DB SELECTION", command=on_select_db)
            TD_1_button.grid(row=10, column=1, columnspan=2, pady=10)
        def create_tables_select():
            # Create ComboBox Widget 
            global tables_list_widget
            tables_file_path = f'{self.dir_path}\\Data\\User_Access_Tables.csv'
            df = pd.read_csv(tables_file_path)
            df = df[df['DataBase'] == self.selected_db]
            table_list = df['Table_Name'].tolist()

            tables_list_widget = ttk.Combobox(credentials_window, values = table_list)
            tables_list_widget.grid(row=11, column=0, columnspan=1, pady=10)
            # Set a Default Value
            tables_list_widget.set('Select a Table')

            # Bind the even 
            TD_1_button = tk.Button(credentials_window, text="Get Table", command=on_select_table)
            TD_1_button.grid(row=11, column=1, columnspan=2, pady=10)
        def teradata_DBs():
            # Check if DataBase exists if not pull data
            tables_file_path = f'{self.dir_path}\\Data\\User_Access_Tables.csv'
            db_file_path = f'{self.dir_path}\\Data\\User_Access_DataBase.csv'
            if ((self.tables is None) and not (os.path.exists(tables_file_path))):
                self.show_message("Warning", "Please Pull Tables First.", "warning")
                return
            elif ((self.tables is not None) or (os.path.exists(tables_file_path))):

                # if no DBs in CACHE 
                if self.tables is None:
                    try:
                        with open(db_file_path, 'r') as file:
                            dbs = file.readlines()
                            if len(dbs) > 2:
                                dbs = [db.split(',')[-1].split('\n')[0] for db in dbs]
                                create_db_select(dbs)

                    except FileNotFoundError:
                        export_teradata_Tables()
                elif self.tables is None:
                    print(self.Tables)
        # Create a new Toplevel window for getting Teradata credentials
        global credentials_window
        credentials_window = tk.Toplevel(self.root)
        credentials_window.title("Teradata Credentials")

        # Create a status bar for the credentials window
        global status_bar_credentials
        status_bar_credentials = tk.Label(credentials_window, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        status_bar_credentials.grid(row=20, column=0, columnspan=2, sticky=tk.W + tk.E)


        # Create labels and entry widgets for username and password
        tk.Label(credentials_window, text="Username:").grid(row=0, column=0, padx=10, pady=5)
        username_entry = tk.Entry(credentials_window)
        username_entry.grid(row=0, column=1, padx=10, pady=5)

        tk.Label(credentials_window, text="Password:").grid(row=1, column=0, padx=10, pady=5)
        password_entry = tk.Entry(credentials_window, show='*')
        password_entry.grid(row=1, column=1, padx=10, pady=5)

        # Connect to TeraData Button
        TD_1_button = tk.Button(credentials_window, text="Connect to TeraData", command=connect_to_teradata)
        TD_1_button.grid(row=2, column=0, columnspan=2, pady=10)

        # Show DBs Button
        TD_2_Button = tk.Button(credentials_window, text="Show DataBases User Has Access To", command=show_teradata_DBs)
        TD_2_Button.grid(row=3, column=0, columnspan=1, pady=10)
        
        # Export DBs Button
        TD_3_Button = tk.Button(credentials_window, text="Export DataBases", command=export_teradata_DBs)
        TD_3_Button.grid(row=3, column=1, columnspan=1, pady=10)

        # Show Tables
        TD_4_Button = tk.Button(credentials_window, text="Show Tables User Has Access To", command=show_teradata_Tables)
        TD_4_Button.grid(row=4, column=0, columnspan=1, pady=10)
        
        # Export Tables
        TD_5_Button = tk.Button(credentials_window, text="Export Tables", command=export_teradata_Tables)
        TD_5_Button.grid(row=4, column=1, columnspan=1, pady=10)

        # Choose Table
        TD_6_Button = tk.Button(credentials_window, text="Get Table Definition ", command=teradata_DBs)
        TD_6_Button.grid(row=5, column=0, columnspan=1, pady=10)
    def show_message(self, title, message, message_type="info"):
        # Display a message box with the specified title and message
        if message_type == "info":
            messagebox.showinfo(title, message)
        elif message_type == "warning":
            messagebox.showwarning(title, message)
    def get_file_path(self, file_type, file_extension, save=False):
        dialog_method = filedialog.asksaveasfilename if save else filedialog.askopenfilename
        file_path = dialog_method(filetypes=[(file_type, file_extension)])
        return file_path 

class CSVToSQLiteConverter(TeradataHandler):
    def __init__(self, root):
        super().__init__(root)
        # Initialize the class with the main Tkinter window
        self.root = root
        self.root.title("CSV to SQLite Converter") # Set the window title
        self.df = None

        # Initialize variables for progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(self.root, variable=self.progress_var, mode="indeterminate")

        # Create the GUI widgets
        self.create_widgets()
        # Bind the callback function to the window close event
        root.protocol("WM_DELETE_WINDOW", self.on_close)
    def on_close(self):
        # Ask for confirmation before closing
        confirmed = messagebox.askokcancel("Confirmation", "Do you want to close the application?")
        
        # This function will be called when the window is closed
        if confirmed:
            try:
                # Close the ODBC connection if it exists
                if self.odbc_connection is not None:
                    self.odbc_connection.close()

                # Destroy the root window
                self.root.destroy()
            except AttributeError:
                # Destroy the root window
                self.root.destroy()
    def create_widgets(self):
        # Create labels and buttons
        self.label = tk.Label(self.root, text="Select CSV File:")
        self.label.pack(pady=10)

        self.browse_button = tk.Button(self.root, text="Load a CSV File", command=self.browse_file)
        self.browse_button.pack(pady=10)

        self.operations_frame = tk.Frame(self.root)
        self.operations_frame.pack(pady=10)

        self.convert_button = tk.Button(self.operations_frame, text="Convert to SQLite", command=self.convert_to_sqlite)
        self.convert_button.pack(pady=10)

        self.load_button = tk.Button(self.operations_frame, text="Load from SQLite", command=self.load_from_sqlite)
        self.load_button.pack(pady=10)

        self.display_button = tk.Button(self.operations_frame, text="Display Top 10", command=self.display_top_10)
        self.display_button.pack(pady=10)

        self.extract_button = tk.Button(self.operations_frame, text="Extract to CSV", command=self.extract_to_csv)
        self.extract_button.pack(pady=10)

        self.clear_button = tk.Button(self.operations_frame, text="Clear Data", command=self.clear_data)
        self.clear_button.pack(side=tk.LEFT, padx=5)

        self.preview_button = tk.Button(self.operations_frame, text="Preview Data", command=self.preview_data)
        self.preview_button.pack(side=tk.LEFT, padx=5)

        self.teradata_button = tk.Button(self.operations_frame, text="Connect to TeraData", command=self.get_credentials_window)
        self.teradata_button.pack(side=tk.LEFT, padx=5)
        
        self.status_bar = tk.Label(self.root, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        self.progress_bar.pack(pady=10)
        # Make the main window resizable
        self.root.resizable(width=True, height=True)
    def update_status_bar(self, message):
        self.status_bar.config(text=message)
        self.root.update_idletasks()
    def browse_file(self):
        # Open a file dialog to select CSV file
        file_path = self.get_file_path("CSV Files", "*.csv")
        if file_path:
            self.label.config(text=f"Selected File: {file_path}")
            self.csv_file_path = file_path
            if self.df is not None:
                overwrite = self.confirm_overwrite("File")
                if not overwrite:
                    self.csv_file_path = None

            self.start_progress_bar()   
            # Read CSV file into a Pandas DataFrame
            self.df = pd.read_csv(self.csv_file_path)
            self.stop_progress_bar()   
            self.update_status_bar("CSV file loaded successfully.")
    def convert_to_sqlite(self):
        # Open a file dialog to choose where to save the SQLite database
        db_path = self.get_file_path("SQLite Database", "*.db", save=True)
       # Check if a location is selected
        if not db_path:
            self.show_message("Warning", "Please select a location to save the SQLite database.", "warning")
            return

        if self.df is None or self.df.empty:
            self.show_message("Warning", "Please load data from CSV first.", "warning")
            return
        # Check if a location is selected
        if db_path:
            if self.df is not None or self.df.empty == False:
                overwrite = self.confirm_overwrite("SQLite Database")
                if not overwrite:
                    return
            # Show progress bar during conversion
            self.start_progress_bar()
            # Create SQLite database and table
            conn = sqlite3.connect(db_path)
            self.df.to_sql('data', conn, index=False, if_exists='replace')
            self.show_message("Success", "CSV data successfully loaded into SQLite.")
            # Hide progress bar after completion
            self.stop_progress_bar()
    def load_from_sqlite(self):
        if self.df is not None or self.df.empty == False:
            overwrite = self.confirm_overwrite("DataFrame")
            if not overwrite:
                return

        # Open a file dialog to select SQLite database
        db_path = self.get_file_path("SQLite Database", "*.db")

        # Check if a database is selected
        if not db_path:
            self.show_message("Warning", "Please select an SQLite database first.", "warning")
            return
        if db_path:
            # Read data from SQLite into a Pandas DataFrame
            conn = sqlite3.connect(db_path)
            query = "SELECT * FROM data"
            try:
                self.df = pd.read_sql_query(query, conn)
                self.show_message("Success", "Data loaded from SQLite.")
            except pd.io.sql.DatabaseError:
                self.show_message("Error", "Error loading data from SQLite database.", "warning")
    def extract_to_csv(self):
        # Check if data is loaded
        if self.df is None or self.df.empty:
            self.show_message("Warning", "Please load data from SQLite first.", "warning")
            return

        # Open a file dialog to choose where to save the CSV file
        csv_path = self.get_file_path("CSV Files", "*.csv", save=True)


        # Check if a location is selected
        if not csv_path:
            self.show_message("Warning", "Please select a location to save the CSV file.", "warning")
            return
        
        if csv_path:
            overwrite = self.confirm_overwrite("CSV File")
            if not overwrite:
                return
            # Show progress bar during extraction
            self.start_progress_bar()            
            # Save DataFrame to CSV
            self.df.to_csv(csv_path, index=False)
            self.show_message("Success", "Data successfully extracted to CSV.")
            # Hide progress bar after completion
            self.stop_progress_bar()
    def display_top_10(self):
        # Check if data is loaded
        if self.df is None or self.df.empty:
            self.show_message("Warning", "Please load data from SQLite first.", "warning")
            return

        # Create a new window for displaying top 10 records
        top10_window = tk.Toplevel(self.root)
        top10_window.title("Top 10 Records")
        
        # Create a Treeview widget for displaying data
        tree = ttk.Treeview(top10_window, columns=self.df.columns, show='headings', height=10)
        #for col in self.df.columns.tolist()[1:]:
            #tree.heading(col, text=col)
        #tree.pack()

        # Insert data into the Treeview
        for i in range(min(10, len(self.df))):
            tree.insert("", "end", values=list(self.df.iloc[i]))
    def clear_data(self):
        self.csv_file_path = None
        self.df = None
        self.label.config(text="Selected File: None")
        self.show_message("Info", "Data cleared.")
    def preview_data(self):
        if self.df is None or self.df.empty:
            self.show_message("Warning", "Please load data from SQLite first.", "warning")
            return

        preview_window = tk.Toplevel(self.root)
        preview_window.title("Data Preview")

        # Display a small sample of the loaded data
        preview_text = tk.Text(preview_window, wrap=tk.WORD)
        preview_text.insert(tk.END, self.df.head())
        preview_text.pack()
    def confirm_overwrite(self, file_type):
        overwrite = messagebox.askyesno(
            "Confirmation",
            f"Are you sure you want to overwrite the existing {file_type}?",
            icon='warning'
        )
        return overwrite
    def start_progress_bar(self):
        # Show and start the progress bar
        self.progress_bar.config(mode="determinate")
        self.progress_bar.start()
    def stop_progress_bar(self):
        self.progress_bar.stop()
        self.progress_bar.config(mode="indeterminate")
        self.update_status_bar("Ready")
    def get_file_path(self, file_type, file_extension, save=False):
        dialog_method = filedialog.asksaveasfilename if save else filedialog.askopenfilename
        file_path = dialog_method(filetypes=[(file_type, file_extension)])
        return file_path 
if __name__ == "__main__":
    root = tk.Tk()
    app = CSVToSQLiteConverter(root)
    root.mainloop()
