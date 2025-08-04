import requests
import json
import csv

BASE_URL = "https://www.ebi.ac.uk/pride/ws/archive/projects"
QUERY = "immunopeptidomics cancer timsTOF cell line tissue xenograft"

# Strict filtering parameters
REQUIRED_INSTRUMENT = "timsTOF"
REQUIRED_TERMS = ["immunopeptidomics", "cancer"]
SAMPLE_TERMS = ["cell line", "tissue", "xenograft"]

# Output files
OUTPUT_JSON = "pride_filtered_immunopeptidomics_timsTOF.json"
OUTPUT_TSV = "pride_filtered_immunopeptidomics_timsTOF.tsv"

def matches_strict_criteria(project):
    """Apply strict filtering based on title, instrument, and sample metadata."""
    title = (project.get("title") or "").lower()
    instruments = [i.lower() for i in (project.get("instrument") or [])]
    diseases = [d.lower() for d in (project.get("diseases") or [])]

    raw_sample = project.get("sample")
    if isinstance(raw_sample, list):
        sample = " ".join(raw_sample).lower()
    elif isinstance(raw_sample, str):
        sample = raw_sample.lower()
    else:
        sample = ""

    # Must mention immunopeptidomics & cancer
    if not all(term in (title + " " + sample + " " + " ".join(diseases)) for term in REQUIRED_TERMS):
        return False
    # Must have timsTOF in instruments
    if not any(REQUIRED_INSTRUMENT.lower() in i for i in instruments):
        return False
    # Must mention cell line, tissue, or xenograft in sample
    if not any(term in sample for term in SAMPLE_TERMS):
        return False
    return True


def get_pride_projects(query, max_pages=20):
    """Query PRIDE API and apply strict filtering."""
    filtered_projects = []
    for page in range(max_pages):
        params = {"q": query, "page": page, "size": 100}
        r = requests.get(BASE_URL, params=params, timeout=60)
        if r.status_code != 200:
            print(f"Error: {r.status_code}")
            break

        data = r.json()
        projects = data.get("list", []) if isinstance(data, dict) else data
        if not projects:
            break

        for p in projects:
            proj = {
                "accession": p.get("accession"),
                "title": p.get("title"),
                "instrument": p.get("instruments"),
                "diseases": p.get("diseases"),
                "sample": p.get("sampleProcessing"),
                "ftpLinks": p.get("ftpLinks")
            }
            if matches_strict_criteria(proj):
                filtered_projects.append(proj)
    return filtered_projects

def save_to_json(data, json_file):
    with open(json_file, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved filtered results to: {json_file}")

def save_to_tsv(data, tsv_file):
    columns = ["accession", "title", "instrument", "diseases", "sample", "ftpLinks"]
    with open(tsv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(columns)
        for proj in data:
            writer.writerow([
                proj.get("accession", ""),
                proj.get("title", ""),
                ", ".join(proj.get("instrument") or []),
                ", ".join(proj.get("diseases") or []),
                ", ".join(proj.get("sample") or []),
                ", ".join(proj.get("ftpLinks") or [])
            ])
    print(f"✅ TSV file saved as: {tsv_file}")

if __name__ == "__main__":
    print("Querying PRIDE API and applying strict filters...")
    results = get_pride_projects(QUERY, max_pages=20)
    print(f"Retrieved {len(results)} STRICTLY matching datasets.")

    # Save outputs
    save_to_json(results, OUTPUT_JSON)
    save_to_tsv(results, OUTPUT_TSV)
