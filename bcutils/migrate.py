import os
import re
import shutil

import pandas as pd


def migrate_to_multi_freq(prices_dir: str, codes: list, dry_run=True):
    print(f"migrate_to_multi_freq() for {prices_dir}, dry run: {dry_run}")

    for instr_code in codes:
        print(f"Processing {instr_code}, dry run: {dry_run}")
        regex = re.compile("^" + instr_code + "_[0-9]{8}.csv")
        file_names = [fn for fn in sorted(os.listdir(prices_dir)) if regex.match(fn)]
        # file_names = ["SEK_20240800.csv"]

        for file in file_names:
            full_path = f"{prices_dir}/{file}"

            print(f"Attempting to parse {full_path}, dry run: {dry_run}")
            df = pd.read_csv(full_path)
            df["Time"] = pd.to_datetime(df["Time"], format="%Y-%m-%dT%H:%M:%S%z")
            df = df.tail(100)  # only consider most recent 100 rows
            ts_diff = df["Time"] - df["Time"].shift()
            ts_diff_mean = ts_diff.mean()
            print(f"{file}: rows: {len(df)}, average shift diff: {ts_diff_mean}")

            if pd.isnull(ts_diff_mean):
                delete(full_path, dry_run, reason="pd.isnull(ts_diff_mean)")
            elif len(df) <= 21:
                delete(full_path, dry_run, reason="len(df) <= 21")
            elif ts_diff_mean.days >= 3:
                delete(full_path, dry_run, reason="ts_diff_mean.days >= 3")
            elif ts_diff_mean.days > 1:
                rename(file, full_path, "Day", prices_dir, dry_run)
            elif ts_diff_mean.days == 1:
                if ts_diff_mean.seconds > 64800:  # 18hrs - arbitrary, test with dry_run
                    delete(
                        full_path,
                        dry_run,
                        reason=f"ts_diff_mean.seconds > 64800 ({ts_diff_mean.seconds})",
                    )
                else:
                    rename(file, full_path, "Day", prices_dir, dry_run)
            elif ts_diff_mean.days == 0:
                if ts_diff_mean.seconds == 0:
                    delete(full_path, dry_run, reason="ts_diff_mean.seconds == 0")
                else:
                    rename(file, full_path, "Hour", prices_dir, dry_run)
            else:
                print(f"Don't know how to handle: {full_path}")


def rename(file, full_path, freq, prices_dir, dry_run=False):
    if dry_run:
        print(f"Would rename: {full_path} to {prices_dir}/{freq}_{file}\n")
    else:
        print(f"Renaming: {full_path} to {prices_dir}/{freq}_{file}\n")
        shutil.move(full_path, f"{prices_dir}/{freq}_{file}")


def delete(full_path, dry_run=False, reason="Dunno"):
    if dry_run:
        print(f"Would delete: {full_path}, because {reason}\n")
    else:
        print(f"Deleting: {full_path}, because {reason}\n")
        os.remove(full_path)


if __name__ == "__main__":
    migrate_to_multi_freq(
        "/home/user/prices/barchart",
        ["SEK"],
        dry_run=True,
    )
