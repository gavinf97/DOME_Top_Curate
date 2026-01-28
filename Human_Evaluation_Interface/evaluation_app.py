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

# Fields to evaluate
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
        
        # Configure overall theme colors and fonts
        self.colors = {
            "bg_main": "#F0F2F5",
            "header_bg": "#FFFFFF",
            "human_bg": "#E3F2FD", # Light Blue
            "human_fg": "#0D47A1", # Darker Blue text
            "copilot_bg": "#E8F5E9", # Light Green
            "copilot_fg": "#1B5E20", # Darker Green text
            "btn_primary": "#1976D2",
            "btn_text": "#FFFFFF",
            "text": "#212121"
        }
        
        # Enhanced Font Settings - Clean, Professional, Legible
        self.base_font = ("Helvetica", 12)
        self.header_font = ("Helvetica", 16, "bold")
        self.label_font = ("Helvetica", 12, "bold")
        self.text_font = ("Helvetica", 13) 
        self.small_font = ("Helvetica", 10)

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

        done_set = set(zip(self.results_df['PMCID'], self.results_df['Field']))
        
        for i, pmcid in enumerate(self.pmc_ids):
            for j, field in enumerate(FIELDS):
                if (pmcid, field) not in done_set:
                    self.current_pmc_index = i
                    self.current_field_index = j
                    return
        
        self.current_pmc_index = len(self.pmc_ids) - 1
        self.current_field_index = len(FIELDS) - 1
        messagebox.showinfo("Complete", "All items have been evaluated (or restarting from end)!")

    def save_result(self, rank, comment):
        pmcid = self.pmc_ids[self.current_pmc_index]
        field = FIELDS[self.current_field_index]
        
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
        
        mask = (self.results_df['PMCID'] == pmcid) & (self.results_df['Field'] == field)
        if mask.any():
            for col, val in new_row.items():
                self.results_df.loc[mask, col] = val
        else:
            self.results_df = pd.concat([self.results_df, pd.DataFrame([new_row])], ignore_index=True)
            
        try:
            self.results_df.to_csv(OUTPUT_FILE, sep='\t', index=False)
            self.results_df.to_csv(BACKUP_FILE, sep='\t', index=False)
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save to file: {e}")

    def load_current_data(self):
        if self.current_pmc_index >= len(self.pmc_ids):
            return

        pmcid = self.pmc_ids[self.current_pmc_index]
        folder_path = self.pmc_folders[self.current_pmc_index]
        
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
            
        self.main_pdf = os.path.join(folder_path, f"{pmcid}_main.pdf")
        self.supp_pdfs = glob.glob(os.path.join(folder_path, "*.pdf"))
        self.supp_pdfs = [p for p in self.supp_pdfs if os.path.abspath(p) != os.path.abspath(self.main_pdf)]
        
        self.update_display()

    def update_display(self):
        pmcid = self.pmc_ids[self.current_pmc_index]
        field = FIELDS[self.current_field_index]
        
        # Update Titles
        self.title_label.config(text=f"PMCID: {pmcid} ({self.current_pmc_index + 1}/{len(self.pmc_ids)})")
        self.subtitle_label.config(text=f"Field: {field} ({self.current_field_index + 1}/{len(FIELDS)})")
        
        # Values
        val_a = str(self.human_data.get(field, "NA"))
        val_b = str(self.copilot_data.get(field, "NA"))
        
        self.text_a.delete("1.0", tk.END)
        self.text_a.insert("1.0", val_a)
        
        self.text_b.delete("1.0", tk.END)
        self.text_b.insert("1.0", val_b)
        
        # Character Counts
        len_a = len(val_a)
        len_b = len(val_b)
        self.label_len_a.config(text=f"Length: {len_a} chars")
        self.label_len_b.config(text=f"Length: {len_b} chars")

        # Load previous rating
        mask = (self.results_df['PMCID'] == pmcid) & (self.results_df['Field'] == field)
        if mask.any():
            row = self.results_df[mask].iloc[0]
            self.rank_var.set(row['Rank'])
            self.comment_entry.delete("1.0", tk.END)
            if pd.notna(row['Comment']):
                self.comment_entry.insert("1.0", str(row['Comment']))
        else:
            self.rank_var.set("")
            self.comment_entry.delete("1.0", tk.END)

        # Update PDF Dropdown
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
        rank = self.rank_var.get()
        if not rank:
            messagebox.showwarning("Input Required", "Please select a rank.")
            return

        comment = self.comment_entry.get("1.0", tk.END).strip()
        self.save_result(rank, comment)
        
        self.current_field_index += 1
        if self.current_field_index >= len(FIELDS):
            self.current_field_index = 0
            self.current_pmc_index += 1
            if self.current_pmc_index >= len(self.pmc_ids):
                messagebox.showinfo("Done", "Evaluation Complete!")
                return
            else:
                 self.load_current_data()
        
        self.update_display()

    def prev_item(self):
        self.current_field_index -= 1
        if self.current_field_index < 0:
            self.current_pmc_index -= 1
            if self.current_pmc_index < 0:
                self.current_pmc_index = 0
                self.current_field_index = 0
            else:
                self.current_field_index = len(FIELDS) - 1
            self.load_current_data()
        self.update_display()

    def setup_ui(self):
        # STYLES - Crucial for font fix
        style = ttk.Style()
        style.theme_use('clam') 
        
        # General Colors
        style.configure("TFrame", background=self.colors["bg_main"])
        style.configure("TLabelframe", background=self.colors["bg_main"])
        # Explicit font for TLabelframe.Label
        style.configure("TLabelframe.Label", background=self.colors["bg_main"], font=self.label_font, foreground=self.colors["text"])
        
        style.configure("TLabel", background=self.colors["bg_main"], font=self.base_font, foreground=self.colors["text"])
        style.configure("TButton", font=self.base_font)
        style.configure("TRadiobutton", background=self.colors["bg_main"], font=self.base_font, foreground=self.colors["text"])
        style.configure("TCombobox", font=self.base_font)
        
        # Header Styles
        style.configure("Header.TLabel", font=self.header_font, foreground="#37474F")
        style.configure("SubHeader.TLabel", font=("Helvetica", 14, "italic"), foreground="#455A64")
        
        # Custom styles for A/B panels
        style.configure("Human.TLabelframe", background=self.colors["human_bg"])
        style.configure("Human.TLabelframe.Label", background=self.colors["human_bg"], foreground=self.colors["human_fg"], font=self.label_font)
        style.configure("Copilot.TLabelframe", background=self.colors["copilot_bg"])
        style.configure("Copilot.TLabelframe.Label", background=self.colors["copilot_bg"], foreground=self.colors["copilot_fg"], font=self.label_font)

        # Main Container
        self.root.configure(bg=self.colors["bg_main"])
        main_frame = ttk.Frame(self.root, padding="15")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # --- Header Section ---
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.title_label = ttk.Label(header_frame, text="PMCID: ...", style="Header.TLabel")
        self.title_label.pack(side=tk.LEFT, padx=(0, 20))
        
        self.subtitle_label = ttk.Label(header_frame, text="Field: ...", style="SubHeader.TLabel")
        self.subtitle_label.pack(side=tk.LEFT)
        
        # --- PDF Controls ---
        pdf_frame = ttk.Labelframe(main_frame, text="Source Documents", padding="10")
        pdf_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(pdf_frame, text="📄 Open Main PDF", command=self.open_main_pdf).pack(side=tk.LEFT, padx=(0, 15))
        
        ttk.Label(pdf_frame, text="Supplementary:").pack(side=tk.LEFT, padx=(0, 5))
        # Combobox font is handled by option_add for drop down list usually, but here handled by style
        self.supp_pdf_combo = ttk.Combobox(pdf_frame, state="readonly", width=40, font=self.base_font)
        self.supp_pdf_combo.pack(side=tk.LEFT, padx=(0, 10))
        self.btn_open_supp = ttk.Button(pdf_frame, text="📎 Open Supp PDF", command=self.open_supp_pdf)
        self.btn_open_supp.pack(side=tk.LEFT)
        
        # --- Comparison Section ---
        comp_frame = ttk.Frame(main_frame)
        comp_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Left: Human (A)
        frame_a = ttk.Labelframe(comp_frame, text="Verify A: Human Annotation", style="Human.TLabelframe", padding="10")
        frame_a.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        self.text_a = scrolledtext.ScrolledText(frame_a, height=15, font=self.text_font, bg=self.colors["header_bg"], padx=10, pady=10)
        self.text_a.pack(fill=tk.BOTH, expand=True)
        
        # Length Label A
        self.label_len_a = ttk.Label(frame_a, text="Length: 0 chars", background=self.colors["human_bg"], foreground="#546E7A", font=self.small_font)
        self.label_len_a.pack(anchor=tk.E, pady=(5, 0))

        # Right: Copilot (B)
        frame_b = ttk.Labelframe(comp_frame, text="Verify B: Copilot Annotation", style="Copilot.TLabelframe", padding="10")
        frame_b.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        self.text_b = scrolledtext.ScrolledText(frame_b, height=15, font=self.text_font, bg=self.colors["header_bg"], padx=10, pady=10)
        self.text_b.pack(fill=tk.BOTH, expand=True)
        
        # Length Label B
        self.label_len_b = ttk.Label(frame_b, text="Length: 0 chars", background=self.colors["copilot_bg"], foreground="#546E7A", font=self.small_font)
        self.label_len_b.pack(anchor=tk.E, pady=(5, 0))

        # --- Rating Section ---
        rating_frame = ttk.Labelframe(main_frame, text="Evaluation", padding="15")
        rating_frame.pack(fill=tk.X, pady=10)
        
        # Rank Options
        self.rank_var = tk.StringVar()
        r_frame = ttk.Frame(rating_frame)
        r_frame.pack(fill=tk.X, pady=(0, 10))
        
        opts = [
            ("A is Better (Human)", "A_Better"),
            ("B is Better (Copilot)", "B_Better"),
            ("Tie (High Quality)", "Tie_High"),
            ("Tie (Low Quality)", "Tie_Low")
        ]
        
        for text, val in opts:
            rb = ttk.Radiobutton(r_frame, text=text, variable=self.rank_var, value=val)
            rb.pack(side=tk.LEFT, padx=(0, 30))
            
        # Comment
        ttk.Label(rating_frame, text="Comments:", font=self.label_font).pack(anchor=tk.W, pady=(5, 0))
        self.comment_entry = tk.Text(rating_frame, height=3, font=self.text_font, padx=5, pady=5)
        self.comment_entry.pack(fill=tk.X, pady=(5, 0))
        
        # --- Navigation Footer ---
        nav_frame = ttk.Frame(main_frame)
        nav_frame.pack(fill=tk.X, pady=15)
        
        ttk.Button(nav_frame, text="<< Previous", command=self.prev_item).pack(side=tk.LEFT)
        
        # Custom button for primary action to ensure it pops
        btn_next = tk.Button(nav_frame, text="Save & Next >>", command=self.next_item, 
                             bg=self.colors["btn_primary"], fg=self.colors["btn_text"], 
                             font=("Helvetica", 14, "bold"), padx=25, pady=8, relief=tk.FLAT)
        btn_next.pack(side=tk.RIGHT)

    @property
    def main_font_family(self):
        return "Helvetica"

if __name__ == "__main__":
    root = tk.Tk()
    # Ensure HighDPI awareness on Windows/Linux if applicable
    try:
        root.tk.call('tk', 'scaling', 1.5) 
    except:
        pass
    app = EvaluationApp(root)
    root.mainloop()
