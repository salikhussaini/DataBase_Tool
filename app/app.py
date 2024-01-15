import tkinter as tk
from tkinter import filedialog, messagebox, ttk    # Import messagebox from tkinter

from tkinter import filedialog
import sqlite3
import pandas as pd
import pyodbc

class CSVToSQLiteConverter:
    def __init__(self, root):
        # Initialize the class with the main Tkinter window
        self.root = root
        self.root.title("CSV to SQLite Converter") # Set the window title
        self.df = None

        self.TD_UserName = None
        self.TD_PassWord = None
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
            # Close the ODBC connection if it exists
            if self.odbc_connection is not None:
                self.odbc_connection.close()

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

        self.teradata_button = tk.Button(self.operations_frame, text="Connect to TeraData", command=self.get_teradata_credentials)
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
    def get_teradata_credentials(self):
        # Create a new Toplevel window for getting Teradata credentials
        credentials_window = tk.Toplevel(self.root)
        credentials_window.title("Teradata Credentials")

        # Create a status bar for the credentials window
        status_bar_credentials = tk.Label(credentials_window, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        status_bar_credentials.grid(row=3, column=0, columnspan=2, sticky=tk.W + tk.E)


        # Create labels and entry widgets for username and password
        tk.Label(credentials_window, text="Username:").grid(row=0, column=0, padx=10, pady=5)
        username_entry = tk.Entry(credentials_window)
        username_entry.grid(row=0, column=1, padx=10, pady=5)

        tk.Label(credentials_window, text="Password:").grid(row=1, column=0, padx=10, pady=5)
        password_entry = tk.Entry(credentials_window, show='*')
        password_entry.grid(row=1, column=1, padx=10, pady=5)
        teradata_username = None
        
        # Function to get credentials when OK button is pressed
        def get_credentials():
            self.TD_UserName = username_entry.get()
            self.TD_PassWord = password_entry.get()
            if self.TD_UserName is '' or self.TD_PassWord is '':
                self.show_message("Warning", "UserName or PassWord Not Entered", "warning")
                return
            else:
                status_bar_credentials.config(text="Credentials entered")
                    
        def connect_to_teradata():
            if self.TD_UserName is not None and self.TD_PassWord is not None:
                try:
                    # Construct the ODBC connection string
                    odbc_connection_string = f"DSN={teradata_dsn};UID={self.TD_UserName};PWD={self.TD_PassWord}"

                    # Establish ODBC connection to Teradata
                    conn = pyodbc.connect(odbc_connection_string)
                    status_bar_credentials.config(text="Connected to Teradata")
                    self.conn = conn
                except pyodbc.Error as e:
                    status_bar_credentials.config(text=f"Error connecting to Teradata: {e}")
                    return None
        
        # OK button to get credentials
        ok_button = tk.Button(credentials_window, text="OK", command=get_credentials)
        ok_button.grid(row=2, column=0, columnspan=1, pady=10)
        ok_button = tk.Button(credentials_window, text="Connect to TeraData", command=connect_to_teradata)
        ok_button.grid(row=2, column=1, columnspan=1, pady=10)

    def get_file_path(self, file_type, file_extension, save=False):
        dialog_method = filedialog.asksaveasfilename if save else filedialog.askopenfilename
        file_path = dialog_method(filetypes=[(file_type, file_extension)])
        return file_path 
    def show_message(self, title, message, message_type="info"):
        # Display a message box with the specified title and message
        if message_type == "info":
            messagebox.showinfo(title, message)
        elif message_type == "warning":
            messagebox.showwarning(title, message)
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
if __name__ == "__main__":
    root = tk.Tk()
    app = CSVToSQLiteConverter(root)
    root.mainloop()
