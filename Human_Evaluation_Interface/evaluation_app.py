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

# DOME Questions Mapping
FIELD_QUESTIONS = {
    "dataset/provenance": [
        "What is the source of the data (database, publication, direct experiment)?",
        "If data are in classes, how many data points are available in each class—for example, total for the positive (Npos) and negative (Nneg) cases?",
        "If regression, how many real value points are there?",
        "Has the dataset been previously used by other papers and/or is it recognised by the community?"
    ],
    "dataset/splits": [
        "How many data points are in the training and test sets?",
        "Was a separate validation set used, and if yes, how large was it?",
        "Are the distributions of data types in the training and test sets different?",
        "Are the distributions of data types in both training and test sets plotted?"
    ],
    "dataset/redundancy": [
        "How were the sets split?",
        "Are the training and test sets independent?",
        "How was this enforced (for example, redundancy reduction to less than X% pairwise identity)?",
        "How does the distribution compare to previously published ML datasets?"
    ],
    "dataset/availability": [
        "Are the data, including the data splits used, released in a public forum?",
        "If yes, where (for example, supporting material, URL) and how (license)?"
    ],
    "optimization/algorithm": [
        "What is the ML algorithm class used?",
        "Is the ML algorithm new?",
        "If yes, why was it chosen over better known alternatives?"
    ],
    "optimization/meta": [
        "Does the model use data from other ML algorithms as input?",
        "If yes, which ones?",
        "Is it clear that training data of initial predictors and meta-predictor are independent of test data for the meta-predictor?"
    ],
    "optimization/encoding": [
        "How were the data encoded and preprocessed for the ML algorithm?"
    ],
    "optimization/parameters": [
        "How many parameters (p) are used in the model?",
        "How was p selected?"
    ],
    "optimization/features": [
        "How many features (f) are used as input?",
        "Was feature selection performed?",
        "If yes, was it performed using the training set only?"
    ],
    "optimization/fitting": [
        "Is p much larger than the number of training points and/or is f large (for example, in classification is p ≫ (Npos+Nneg) and/or f > 100)?",
        "If yes, how was overfitting ruled out?",
        "Conversely, if the number of training points is much larger than p and/or f is small (for example, (Npos+Nneg) ≫ p and/or f < 5), how was underfitting ruled out?"
    ],
    "optimization/regularization": [
        "Were any overfitting prevention techniques used (for example, early stopping using a validation set)?",
        "If yes, which ones?"
    ],
    "optimization/config": [
        "Are the hyperparameter configurations, optimisation schedule, model files and optimisation parameters reported?",
        "If yes, where (for example, URL) and how (license)?"
    ],
    "model/interpretability": [
        "Is the model black box or interpretable?",
        "If the model is interpretable, can you give clear examples of this?"
    ],
    "model/output": [
        "Is the model classification or regression?"
    ],
    "model/duration": [
        "How much time does a single representative prediction require on a standard machine (for example, seconds on a desktop PC or high-performance computing cluster)?"
    ],
    "model/availability": [
        "Is the source code released?",
        "Is a method to run the algorithm—such as executable, web server, virtual machine or container instance—released?",
        "If yes, where (for example, URL) and how (license)?"
    ],
    "evaluation/method": [
        "How was the method evaluated (for example cross-validation, independent dataset, novel experiments)?"
    ],
    "evaluation/measure": [
        "Which performance metrics are reported?",
        "Is this set representative (for example, compared to the literature)?"
    ],
    "evaluation/comparison": [
        "Was a comparison to publicly available methods performed on benchmark datasets?",
        "Was a comparison to simpler baselines performed?"
    ],
    "evaluation/confidence": [
        "Do the performance metrics have confidence intervals?",
        "Are the results statistically significant to claim that the method is superior to others and baselines?"
    ],
    "evaluation/availability": [
        "Are the raw evaluation files (for example, assignments for comparison and baselines, statistical code, confusion matrices) available?",
        "If yes, where (for example, URL) and how (license)?"
    ]
}

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

        self.results_df = self.load_existing_results()

        # Data State
        all_folders = glob.glob(os.path.join(DATA_DIR, "PMC*"))
        
        # Prioritize done PMCs to maintain user progress sequence
        done_pmcs = []
        if not self.results_df.empty:
             done_pmcs = self.results_df['PMCID'].unique().tolist()

        def sort_key(folder_path):
            pmc_id = os.path.basename(folder_path)
            if pmc_id in done_pmcs:
                # Group 0: Already done, maintain order of completion
                return (0, done_pmcs.index(pmc_id))
            else:
                # Group 1: New items, sorted alphabetically
                return (1, pmc_id)

        self.pmc_folders = sorted(all_folders, key=sort_key)
        self.pmc_ids = [os.path.basename(f) for f in self.pmc_folders]
        
        if not self.pmc_ids:
            messagebox.showerror("Error", f"No PMC folders found in {DATA_DIR}")
            root.destroy()
            return
        
        # Navigation State
        self.current_pmc_index = 0
        self.current_field_index = 0
        self.find_first_incomplete()
        
        # UI Setup
        self.load_curator_mappings()
        self.setup_ui()
        self.load_current_data()
        self.update_display()

    def load_curator_mappings(self):
        self.doi_to_user_oid = {}
        self.user_details = {}
        
        # Paths - DATA_DIR is "../30_human_evaluation"
        # We need to go up one level from DATA_DIR to find DOME_Registry_JSON_Files
        workspace_root = os.path.dirname(DATA_DIR) # "../"
        raw_reviews_path = os.path.join(workspace_root, "DOME_Registry_JSON_Files", "dome_review_raw_human_20260128.json")
        users_path = os.path.join(workspace_root, "DOME_Registry_JSON_Files", "dome_users_20260130.json")
        
        # Verify paths - the script runs from Human_Evaluation_Interface/
        # DATA_DIR is relative to script location. 
        # Actually, let's use os.path.abspath to be safe.
        script_dir = os.path.dirname(os.path.abspath(__file__))
        registry_dir = os.path.join(os.path.dirname(script_dir), "DOME_Registry_JSON_Files")
        
        raw_reviews_path = os.path.join(registry_dir, "dome_review_raw_human_20260128.json")
        users_path = os.path.join(registry_dir, "dome_users_20260130.json")

        print(f"Loading curator mappings from {users_path}...")

        # Load DOI -> OID map
        try:
            if os.path.exists(raw_reviews_path):
                with open(raw_reviews_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for entry in data:
                        doi = entry.get('publication', {}).get('doi', '')
                        user_oid = entry.get('user', {}).get('$oid', '')
                        if doi and user_oid:
                            self.doi_to_user_oid[doi.strip()] = user_oid
            else:
                print(f"Warning: Raw reviews file not found at {raw_reviews_path}")
        except Exception as e:
            print(f"Error loading raw reviews for mapping: {e}")

        # Load OID -> User Details
        try:
            if os.path.exists(users_path):
                with open(users_path, 'r', encoding='utf-8') as f:
                    users = json.load(f)
                    for u in users:
                        oid = u.get('_id', {}).get('$oid', '')
                        if oid:
                            self.user_details[oid] = {
                                'name': u.get('name', ''),
                                'surname': u.get('surname', ''),
                                'email': u.get('email', ''),
                                'orcid': u.get('orcid', ''),
                                'roles': u.get('roles', '')
                            }
            else:
                print(f"Warning: Users file not found at {users_path}")
        except Exception as e:
            print(f"Error loading users: {e}")

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
        except Exception as e:
            print(f"Error loading human json: {e}")
            self.human_data = {}
            
        try:
            with open(copilot_json_path, 'r') as f:
                self.copilot_data = json.load(f)
        except Exception as e:
            print(f"Error loading copilot json: {e}")
            self.copilot_data = {}
            
        self.main_pdf = os.path.join(folder_path, f"{pmcid}_main.pdf")
        self.supp_pdfs = glob.glob(os.path.join(folder_path, "*.pdf"))
        self.supp_pdfs = [p for p in self.supp_pdfs if os.path.abspath(p) != os.path.abspath(self.main_pdf)]
        
        # self.update_display() # Moved to explicit call to prevent double updates

    def update_display(self):
        if self.current_pmc_index >= len(self.pmc_ids):
            return

        pmcid = self.pmc_ids[self.current_pmc_index]
        field = FIELDS[self.current_field_index]
        
        # Update Titles
        self.title_label.config(text=f"PMCID: {pmcid} ({self.current_pmc_index + 1}/{len(self.pmc_ids)})")
        self.subtitle_label.config(text=f"Field: {field} ({self.current_field_index + 1}/{len(FIELDS)})")

        # Curator Update
        curator_text = "Curator: Unknown"
        current_doi = str(self.human_data.get('publication/doi', '')).strip()
        
        if current_doi in self.doi_to_user_oid:
            user_oid = self.doi_to_user_oid[current_doi]
            if user_oid in self.user_details:
                u = self.user_details[user_oid]
                name = f"{u['name']} {u['surname']}".strip()
                if not name: name = "Unknown Name"
                email = u['email'] if u['email'] else ""
                orcid = u['orcid'] if u['orcid'] else ""
                
                curator_text = f"Curator: {name}"
                if email: curator_text += f" | {email}"
                if orcid: curator_text += f" | ORCID: {orcid}"
        else:
             curator_text = f"Curator: Unknown (DOI: {current_doi})"
        
        self.curator_label.config(text=curator_text)
        
        # Update Questions
        questions = FIELD_QUESTIONS.get(field, [])
        if questions:
            q_text = "\n".join([f"• {q}" for q in questions])
            self.question_label.config(text=q_text, foreground="#004D40") # Dark teal for guidance
            self.question_frame.pack(fill=tk.X, pady=5, before=self.pdf_frame) # Ensure it stays visible
        else:
            self.question_label.config(text="No specific questions for this field.", foreground="#78909C")

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
        
        # Advance logic
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
        self.root.update_idletasks() # Force UI update

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
        self.root.update_idletasks() # Force UI update

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
        
        top_row = ttk.Frame(header_frame)
        top_row.pack(fill=tk.X)

        self.title_label = ttk.Label(top_row, text="PMCID: ...", style="Header.TLabel")
        self.title_label.pack(side=tk.LEFT, padx=(0, 20))
        
        self.subtitle_label = ttk.Label(top_row, text="Field: ...", style="SubHeader.TLabel")
        self.subtitle_label.pack(side=tk.LEFT)
        
        self.curator_label = ttk.Label(header_frame, text="Curator: ...", font=("Helvetica", 11, "bold"), foreground="#1565C0")
        self.curator_label.pack(anchor=tk.W, pady=(5, 0))
        
        # --- Navigation Top (Moved from Footer) ---
        nav_frame = ttk.Frame(main_frame)
        nav_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(nav_frame, text="<< Previous", command=self.prev_item).pack(side=tk.LEFT)
        
        # Custom button for primary action to ensure it pops
        btn_next = tk.Button(nav_frame, text="Save & Next >>", command=self.next_item, 
                             bg=self.colors["btn_primary"], fg=self.colors["btn_text"], 
                             font=("Helvetica", 14, "bold"), padx=25, pady=8, relief=tk.FLAT)
        btn_next.pack(side=tk.RIGHT)

        # --- Question / Guidance Section ---
        self.question_frame = ttk.Labelframe(main_frame, text="DOME Guidelines / Questions", padding="10")
        self.question_frame.pack(fill=tk.X, pady=5)

        
        self.question_label = ttk.Label(self.question_frame, text="", wraplength=1300, justify=tk.LEFT)
        self.question_label.pack(anchor=tk.W)

        # --- PDF Controls ---
        self.pdf_frame = ttk.Labelframe(main_frame, text="Source Documents", padding="10")
        self.pdf_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(self.pdf_frame, text="📄 Open Main PDF", command=self.open_main_pdf).pack(side=tk.LEFT, padx=(0, 15))
        
        ttk.Label(self.pdf_frame, text="Supplementary:").pack(side=tk.LEFT, padx=(0, 5))
        # Combobox font is handled by option_add for drop down list usually, but here handled by style
        self.supp_pdf_combo = ttk.Combobox(self.pdf_frame, state="readonly", width=40, font=self.base_font)
        self.supp_pdf_combo.pack(side=tk.LEFT, padx=(0, 10))
        self.btn_open_supp = ttk.Button(self.pdf_frame, text="📎 Open Supp PDF", command=self.open_supp_pdf)
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

        # Note: Navigation moved to the top

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
