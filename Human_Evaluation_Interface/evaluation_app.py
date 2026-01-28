import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import os
import json
import pandas as pd
import glob
import subprocess
import sys
from datetime import datetime

# Configuration
DATA_DIR = "../30_human_evaluation"
OUTPUT_FILE = "evaluation_results.tsv"
BACKUP_FILE = "evaluation_results_backup.tsv"

# Fields to evaluate (same as reformatting script)
FIELDS = [
  "publication/title",
  "publication/authors",
  "publication/journal",
  "publication/year",
  "publication/doi",
  "publication/tags",
  "dataset/provenance",
  "dataset/splits",
  "dataset/redundancy",
  "dataset/availability",
  "optimization/algorithm",
  "optimization/meta",
  "optimization/encoding",
  "optimization/parameters",
  "optimization/features",
  "optimization/fitting",
  "optimization/regularization",
  "optimization/config",
  "model/interpretability",
  "model/output",
  "model/duration",
  "model/availability",
  "evaluation/method",
  "evaluation/measure",
  "evaluation/comparison",
  "evaluation/confidence",
  "evaluation/availability"
]

class EvaluationApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DOME Human Evaluation Interface")
        self.root.geometry("1400x900")
        
        # Data State
        self.pmc_folders = sorted(glob.glob(os.path.join(DATA_DIR, "PMC*")))
        self.pmc_ids = [os.path.basename(f) for f in self.pmc_folders]
        
        if not self.pmc_ids:
            messagebox.showerror("Error", f"No PMC folders found in {DATA_DIR}")
            root.destroy()
            return

        self.results_df = self.load_existing_results()
        
        # Navigation State
        self.current_pmc_index = 0
        self.current_field_index = 0
        self.find_first_incomplete()
        
        # UI Setup
        self.setup_ui()
        self.load_current_data()

    def load_existing_results(self):
        if os.path.exists(OUTPUT_FILE):
            try:
                return pd.read_csv(OUTPUT_FILE, sep='\t')
            except Exception as e:
                messagebox.showerror("Error", f"Could not load existing TSV: {e}")
                return pd.DataFrame(columns=['PMCID', 'Field', 'Value_A_Human', 'Value_B_Copilot', 'Rank', 'Comment', 'Timestamp'])
        else:
            return pd.DataFrame(columns=['PMCID', 'Field', 'Value_A_Human', 'Value_B_Copilot', 'Rank', 'Comment', 'Timestamp'])

    def find_first_incomplete(self):
        """Find the first PMC/Field combination that hasn't been rated yet."""
        if self.results_df.empty:
            self.current_pmc_index = 0
            self.current_field_index = 0
            return

        # Create a set of (pmcid, field) tuples that are done
        done_set = set(zip(self.results_df['PMCID'], self.results_df['Field']))
        
        for i, pmcid in enumerate(self.pmc_ids):
            for j, field in enumerate(FIELDS):
                if (pmcid, field) not in done_set:
                    self.current_pmc_index = i
                    self.current_field_index = j
                    return
        
        # If all done
        self.current_pmc_index = len(self.pmc_ids) - 1
        self.current_field_index = len(FIELDS) - 1
        messagebox.showinfo("Complete", "All items have been evaluated!")

    def save_result(self, rank, comment):
        pmcid = self.pmc_ids[self.current_pmc_index]
        field = FIELDS[self.current_field_index]
        
        # Current Values
        val_a = self.human_data.get(field, "NA")
        val_b = self.copilot_data.get(field, "NA")
        
        timestamp = datetime.now().isoformat()
        
        new_row = {
            'PMCID': pmcid,
            'Field': field,
            'Value_A_Human': val_a,
            'Value_B_Copilot': val_b,
            'Rank': rank,
            'Comment': comment,
            'Timestamp': timestamp
        }
        
        # Check if exists and update, else append
        mask = (self.results_df['PMCID'] == pmcid) & (self.results_df['Field'] == field)
        if mask.any():
            # Update
            for col, val in new_row.items():
                self.results_df.loc[mask, col] = val
        else:
            # Append
            self.results_df = pd.concat([self.results_df, pd.DataFrame([new_row])], ignore_index=True)
            
        # Save to disk
        try:
            self.results_df.to_csv(OUTPUT_FILE, sep='\t', index=False)
            self.results_df.to_csv(BACKUP_FILE, sep='\t', index=False) # Single file backup
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save to file: {e}")

    def load_current_data(self):
        if self.current_pmc_index >= len(self.pmc_ids):
            return

        pmcid = self.pmc_ids[self.current_pmc_index]
        folder_path = self.pmc_folders[self.current_pmc_index]
        
        # Load JSONs
        human_json_path = os.path.join(folder_path, f"{pmcid}_human.json")
        copilot_json_path = os.path.join(folder_path, f"{pmcid}_copilot.json")
        
        try:
            with open(human_json_path, 'r') as f:
                self.human_data = json.load(f)
        except:
            self.human_data = {}
            
        try:
            with open(copilot_json_path, 'r') as f:
                self.copilot_data = json.load(f)
        except:
            self.copilot_data = {}
            
        # Find PDFs
        self.main_pdf = os.path.join(folder_path, f"{pmcid}_main.pdf")
        self.supp_pdfs = glob.glob(os.path.join(folder_path, "*.pdf"))
        # Exclude main pdf from supp list if present twice (glob catches all)
        self.supp_pdfs = [p for p in self.supp_pdfs if os.path.abspath(p) != os.path.abspath(self.main_pdf)]
        
        self.update_display()

    def update_display(self):
        pmcid = self.pmc_ids[self.current_pmc_index]
        field = FIELDS[self.current_field_index]
        
        # Title
        self.title_label.config(text=f"PMCID: {pmcid} ({self.current_pmc_index + 1}/{len(self.pmc_ids)})")
        self.subtitle_label.config(text=f"Field: {field} ({self.current_field_index + 1}/{len(FIELDS)})")
        
        # Values
        val_a = self.human_data.get(field, "NA")
        val_b = self.copilot_data.get(field, "NA")
        
        self.text_a.delete("1.0", tk.END)
        self.text_a.insert("1.0", str(val_a))
        
        self.text_b.delete("1.0", tk.END)
        self.text_b.insert("1.0", str(val_b))
        
        # Load previous rating if exists
        mask = (self.results_df['PMCID'] == pmcid) & (self.results_df['Field'] == field)
        if mask.any():
            row = self.results_df[mask].iloc[0]
            self.rank_var.set(row['Rank'])
            self.comment_entry.delete("1.0", tk.END)
            if pd.notna(row['Comment']):
                self.comment_entry.insert("1.0", str(row['Comment']))
        else:
            self.rank_var.set("") # Clear selection
            self.comment_entry.delete("1.0", tk.END)

        # Update Supp PDF Dropdown
        self.supp_pdf_combo['values'] = [os.path.basename(p) for p in self.supp_pdfs]
        if self.supp_pdfs:
            self.supp_pdf_combo.current(0)
            self.btn_open_supp.config(state=tk.NORMAL)
        else:
            self.supp_pdf_combo.set("No Supplementary PDFs")
            self.btn_open_supp.config(state=tk.DISABLED)

    def open_main_pdf(self):
        if os.path.exists(self.main_pdf):
            self.open_file(self.main_pdf)
        else:
            messagebox.showwarning("File Missing", "Main PDF not found.")

    def open_supp_pdf(self):
        selection_idx = self.supp_pdf_combo.current()
        if selection_idx >= 0 and selection_idx < len(self.supp_pdfs):
            path = self.supp_pdfs[selection_idx]
            self.open_file(path)

    def open_file(self, filepath):
        try:
            if sys.platform.startswith('linux'):
                subprocess.call(['xdg-open', filepath])
            elif sys.platform.startswith('darwin'):
                subprocess.call(['open', filepath])
            elif sys.platform.startswith('win'):
                os.startfile(filepath)
        except Exception as e:
            messagebox.showerror("Error", f"Could not open PDF: {e}")

    def next_item(self):
        # Validate input
        rank = self.rank_var.get()
        if not rank:
            messagebox.showwarning("Input Required", "Please select a rank.")
            return

        comment = self.comment_entry.get("1.0", tk.END).strip()
        
        # Save
        self.save_result(rank, comment)
        
        # Increment
        self.current_field_index += 1
        if self.current_field_index >= len(FIELDS):
            self.current_field_index = 0
            self.current_pmc_index += 1
            if self.current_pmc_index >= len(self.pmc_ids):
                messagebox.showinfo("Done", "Evaluation Complete!")
                return
            else:
                 # Load new PMC data
                 self.load_current_data()
        
        self.update_display()

    def prev_item(self):
        # Decrement
        self.current_field_index -= 1
        if self.current_field_index < 0:
            self.current_pmc_index -= 1
            if self.current_pmc_index < 0:
                self.current_pmc_index = 0
                self.current_field_index = 0
            else:
                self.current_field_index = len(FIELDS) - 1
            
            # Load new PMC data (since we might have changed PMC)
            self.load_current_data()
            
        self.update_display()

    def setup_ui(self):
        # Styles
        style = ttk.Style()
        style.configure("TLabel", font=("Helvetica", 12))
        style.configure("TButton", font=("Helvetica", 11))
        style.configure("Header.TLabel", font=("Helvetica", 14, "bold"))
        
        # Main Container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # --- Header Section ---
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, pady=5)
        
        self.title_label = ttk.Label(header_frame, text="PMCID: Loading...", style="Header.TLabel")
        self.title_label.pack(side=tk.LEFT, padx=5)
        
        self.subtitle_label = ttk.Label(header_frame, text="Field: Loading...", font=("Helvetica", 12, "italic"))
        self.subtitle_label.pack(side=tk.LEFT, padx=15)
        
        # --- PDF Controls ---
        pdf_frame = ttk.Labelframe(main_frame, text="Source Documents", padding="5")
        pdf_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(pdf_frame, text="Open Main PDF", command=self.open_main_pdf).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(pdf_frame, text="Supplementary:").pack(side=tk.LEFT, padx=10)
        self.supp_pdf_combo = ttk.Combobox(pdf_frame, state="readonly", width=40)
        self.supp_pdf_combo.pack(side=tk.LEFT, padx=5)
        self.btn_open_supp = ttk.Button(pdf_frame, text="Open Supp PDF", command=self.open_supp_pdf)
        self.btn_open_supp.pack(side=tk.LEFT, padx=5)
        
        # --- Comparison Section ---
        comp_frame = ttk.Frame(main_frame)
        comp_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Left: Human (A)
        frame_a = ttk.Labelframe(comp_frame, text="Verify A: Human Annotation", padding="5")
        frame_a.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        self.text_a = scrolledtext.ScrolledText(frame_a, height=15, font=("Consolas", 11))
        self.text_a.pack(fill=tk.BOTH, expand=True)
        
        # Right: Copilot (B)
        frame_b = ttk.Labelframe(comp_frame, text="Verify B: Copilot Annotation", padding="5")
        frame_b.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        self.text_b = scrolledtext.ScrolledText(frame_b, height=15, font=("Consolas", 11))
        self.text_b.pack(fill=tk.BOTH, expand=True)
        
        # --- Rating Section ---
        rating_frame = ttk.Labelframe(main_frame, text="Rank & Comment", padding="10")
        rating_frame.pack(fill=tk.X, pady=5)
        
        # Radio Buttons
        self.rank_var = tk.StringVar()
        r_frame = ttk.Frame(rating_frame)
        r_frame.pack(fill=tk.X)
        
        opts = [
            ("A is Better (Human has fewer errors)", "A_Better"),
            ("B is Better (Copilot has fewer errors)", "B_Better"),
            ("Tie - High Quality (Both perfect)", "Tie_High"),
            ("Tie - Low Quality (Both failed)", "Tie_Low")
        ]
        
        for text, val in opts:
            ttk.Radiobutton(r_frame, text=text, variable=self.rank_var, value=val).pack(side=tk.LEFT, padx=20)
            
        # Comment
        ttk.Label(rating_frame, text="Comments:").pack(anchor=tk.W, pady=(10, 0))
        self.comment_entry = tk.Text(rating_frame, height=3, font=("Helvetica", 11))
        self.comment_entry.pack(fill=tk.X, pady=5)
        
        # --- Navigation Footer ---
        nav_frame = ttk.Frame(main_frame)
        nav_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(nav_frame, text="<< Previous", command=self.prev_item).pack(side=tk.LEFT)
        ttk.Button(nav_frame, text="Save & Next >>", command=self.next_item).pack(side=tk.RIGHT)

if __name__ == "__main__":
    root = tk.Tk()
    app = EvaluationApp(root)
    root.mainloop()
