import gspread
import json
import requests
import csv
import os
import logging
import time
import argparse
import getpass
from typing import List, Dict, Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime, timedelta, timezone

def get_required_input(prompt, is_password=False):
    """Get required input from user with clear prompting."""
    while True:
        if is_password:
            value = getpass.getpass(prompt)
        else:
            value = input(prompt)
        if value.strip():
            return value
        print("This field is required. Please try again.")

def parse_arguments():
    """Parse arguments with interactive fallback for missing required values."""
    parser = argparse.ArgumentParser(description='Calendar Manager Configuration')
    
    parser.add_argument('--api-url', help='Base API URL')
    parser.add_argument('--spreadsheet-url', help='Google Spreadsheet URL')
    parser.add_argument('--username', help='API username')
    parser.add_argument('--password', help='API password')
    
    args = parser.parse_args()
    
    # Interactive prompts for any missing arguments
    if not args.api_url:
        args.api_url = get_required_input("Enter API URL (e.g., https://servizi.comune.bugliano.pi.it/lang/api): ")
    if not args.spreadsheet_url:
        args.spreadsheet_url = get_required_input("Enter Google Spreadsheet URL: ")
    if not args.username:
        args.username = get_required_input("Enter API username: ")
    if not args.password:
        args.password = get_required_input("Enter API password: ", is_password=True)
    
    return args

def show_usage_instructions():
    """Display clear instructions for running the script."""
    print("\n" + "="*60)
    print("CALENDAR MANAGER CONFIGURATION".center(60))
    print("="*60)
    print("\nYou need to provide the following information:")
    print("1. API URL (e.g., https://servizi.comune.bugliano.pi.it/lang/api)")
    print("2. Google Spreadsheet URL")
    print("3. API username")
    print("4. API password")
    print("\nYou can provide these either:")
    print("- As command line arguments (--api-url, --spreadsheet-url, etc.)")
    print("- Or interactively when prompted")
    print("\nExample command line usage:")
    print('python script.py --api-url "https://..." --spreadsheet-url "https://..." \\')
    print('                 --username "your_username" --password "your_password"')
    print("="*60 + "\n")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CalendarManager:
    def __init__(self, api_url, spreadsheet_url, username, password):
        """Initialize with required configuration parameters."""
        try:
            self.gc = gspread.service_account()
            self.BASE_API_URL = api_url
            self.SPREADSHEET_URL = spreadsheet_url
            self.API_CREDENTIALS = {
                "username": username,
                "password": password
            }
            self.token = self.get_token()
            self.headers = {"Authorization": f"Bearer {self.token}"}
            self.calendar_groups = []
            
            # Set date range
            self.start_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            self.end_date = (datetime.now(timezone.utc) + timedelta(days=365)).strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    def get_token(self):
        """Get authentication token from API."""
        endpoint_auth = f"{self.BASE_API_URL}/auth"
        try:
            response = requests.post(
                endpoint_auth,
                json=self.API_CREDENTIALS,
                timeout=10
            )
            response.raise_for_status()
            return response.json()["token"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get token: {e}")
            raise

    def is_valid_time(self, time_str: str) -> bool:
        """Check if a string is a valid time in HH:MM format or 'Chiuso'."""
        if time_str.lower() == "chiuso":
            return True
        try:
            datetime.strptime(time_str, '%H:%M')
            return True
        except ValueError:
            return False

    def clean_spreadsheet_data(self, data: List[List[str]]) -> List[List[str]]:
        """Clean and normalize spreadsheet data."""
        cleaned = []
        for row in data:
            cleaned_row = []
            for cell in row:
                # Normalize "Chiuso" variations and empty strings
                cell_str = str(cell).strip()
                if cell_str.lower() in ["chiuso", "closed", ""]:
                    cleaned_row.append("Chiuso")
                else:
                    cleaned_row.append(cell_str)
            cleaned.append(cleaned_row)
        return cleaned

    def group_calendar_data(self, data: List[List[str]]) -> None:
        """Group calendar data based on empty cells below calendar names."""
        current_group = []
        current_name = ""
        
        for i, row in enumerate(data):
            if not row[0].strip() or row[0].strip() == "Chiuso":  # Empty calendar name or "Chiuso" means continuation
                if current_name:  # Only add if we have a calendar name
                    current_group.append(row)
            else:
                # Save previous group if exists
                if current_name:
                    self.calendar_groups.append((current_name, current_group))
                
                # Start new group
                current_name = row[0]
                current_group = [row]
        
        # Add the last group
        if current_name:
            self.calendar_groups.append((current_name, current_group))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def read_spreadsheet(self, url: str) -> bool:
        """Read data from Google Spreadsheet and group calendar data."""
        try:
            sh = self.gc.open_by_url(url)
            data = sh.sheet1.get_all_values()
            
            if not data or len(data) < 2:
                logger.warning("No data found in spreadsheet")
                return False

            # Remove header rows (first 2 rows)
            data = data[2:]
            cleaned_data = self.clean_spreadsheet_data(data)
            self.group_calendar_data(cleaned_data)
            return True
            
        except Exception as e:
            logger.error(f"Error reading spreadsheet: {e}")
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get_admin_id(self) -> str:
        """Get the ID of the admin user."""
        try:
            ep_get_admin = f"{self.BASE_API_URL}/users?roles=admin"
            response = requests.get(ep_get_admin, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json()[0]["id"]
        except Exception as e:
            logger.error(f"Failed to get admin ID: {e}")
            raise

    def process_opening_hours(self, group: List[List[str]]) -> Dict[Tuple[str, str, str, str], List[int]]:
        """Process opening hours with correct day-to-column mapping."""
        time_slots = {}
        
        for i, row in enumerate(group):
            # Correct day mapping based on the clarified structure:
            # Columns:
            # 0 = CalendarName
            # 1 = Empty
            # 2-4 = Monday (day 1)
            # 5-7 = Tuesday (day 2)
            # 8-10 = Wednesday (day 3)
            # 11-13 = Thursday (day 4)
            # 14-16 = Friday (day 5)
            # 17-19 = Saturday (day 6)
            
            days_mapping = [
                (1, 2),   # Monday (day 1) - columns 2-4
                (2, 5),    # Tuesday (day 2) - columns 5-7
                (3, 8),    # Wednesday (day 3) - columns 8-10
                (4, 11),   # Thursday (day 4) - columns 11-13
                (5, 14),   # Friday (day 5) - columns 14-16
                (6, 17)    # Saturday (day 6) - columns 17-19
            ]
            
            for day_num, day_col in days_mapping:
                # Check if we have enough columns (need 3: open, close, duration)
                if day_col + 2 >= len(row):
                    logger.warning(f"Incomplete data in row {i}, day {day_num} (columns {day_col}-{day_col+2})")
                    continue
                    
                begin_hour = str(row[day_col]).strip()
                end_hour = str(row[day_col + 1]).strip()
                meeting_minutes = str(row[day_col + 2]).strip()
                
                # Skip if closed or empty
                if begin_hour == "Chiuso" or end_hour == "Chiuso" or not begin_hour or not end_hour:
                    continue
                    
                # Validate time format
                if not (self.is_valid_time(begin_hour) and self.is_valid_time(end_hour)):
                    logger.warning(f"Invalid time format in row {i}, day {day_num}: {begin_hour}-{end_hour}")
                    continue
                
                # Default to 30 minutes if meeting duration is invalid
                try:
                    meeting_min = int(meeting_minutes) if meeting_minutes.isdigit() else 30
                except ValueError:
                    meeting_min = 30
                    logger.warning(f"Invalid meeting minutes '{meeting_minutes}', using default 30")
                
                # Create a unique key for this time slot
                slot_key = (begin_hour, end_hour, str(meeting_min), "no")
                
                # Add the day to this time slot's days list
                if slot_key not in time_slots:
                    time_slots[slot_key] = []
                if day_num not in time_slots[slot_key]:  # Prevent duplicate days
                    time_slots[slot_key].append(day_num)
                    
        return time_slots

    def create_opening_hours(self, id_cal: str, time_slots: Dict[Tuple[str, str, str, str], List[int]]) -> bool:
        """Create opening hours for a calendar with grouped days for identical time slots."""
        ep_openings = f"{self.BASE_API_URL}/calendars/{id_cal}/opening-hours"
        success = True
        
        for (begin_hour, end_hour, meeting_min, is_moderated), days in time_slots.items():
            try:
                opening_hours = {
                    "name": f"Orario {begin_hour}-{end_hour}",
                    "start_date": self.start_date,
                    "end_date": self.end_date,
                    "days_of_week": sorted(days),
                    "begin_hour": begin_hour,
                    "end_hour": end_hour,
                    "is_moderated": is_moderated.lower() == "si",
                    "meeting_minutes": int(meeting_min),
                    "interval_minutes": 0,
                    "meeting_queue": 1,
                }

                response = requests.post(
                    ep_openings,
                    json=opening_hours,
                    headers=self.headers,
                    timeout=10
                )
                response.raise_for_status()
                
                logger.info(f"Created opening hours for calendar {id_cal}: Days {days} {begin_hour}-{end_hour}")
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Failed to create opening hours for calendar {id_cal}: {e}")
                success = False
                
        return success

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def create_calendar(self, name: str, group: List[List[str]]) -> Optional[str]:
        """Create a calendar with its opening hours."""
        try:
            logger.info(f"Creating calendar: {name}")

            id_admin = self.get_admin_id()
            
            calendar = {
                "owner": id_admin,
                "code_generation_strategy_id": "",
                "moderators": [],
                "opening_hours": [],
                "title": name,
                "type": "time_fixed_slots",
                "contact_email": "",
                "rolling_days": 90,
                "drafts_duration": 10,
                "drafts_duration_increment": 5,
                "minimum_scheduling_notice": 24,
                "allow_cancel_days": 3,
                "is_moderated": False,
                "location": "Verona",
                "external_calendars": [],
                "closing_periods": [],
                "reservation_limits": []
            }

            response = requests.post(
                f"{self.BASE_API_URL}/calendars",
                json=calendar,
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            
            id_calendar = response.json()["id"]
            logger.info(f"Created calendar {name} with ID: {id_calendar}")

            # Save calendar ID to CSV
            with open('calendars_ids.csv', mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([id_calendar, name])

            # Process and create opening hours
            time_slots = self.process_opening_hours(group)
            if not time_slots:
                logger.warning(f"No valid opening hours found for calendar {name}")
            else:
                self.create_opening_hours(id_calendar, time_slots)
            
            time.sleep(2)  # Rate limiting between calendar creations
            
            return id_calendar
            
        except Exception as e:
            logger.error(f"Failed to create calendar {name}: {e}")
            return None

    def run(self) -> None:
        """Main execution method."""
        try:
            # Initialize CSV file with headers
            with open('calendars_ids.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["Calendar ID", "Calendar Name"])

            if not self.read_spreadsheet(self.SPREADSHEET_URL):
                logger.error("No data available to process")
                return

            for name, group in self.calendar_groups:
                self.create_calendar(name, group)
                
        except Exception as e:
            logger.error(f"Application failed: {e}")
            raise


if __name__ == "__main__":
    show_usage_instructions()
    try:
        args = parse_arguments()
        
        print("\nConfiguration received:")
        print(f"API URL: {args.api_url}")
        print(f"Spreadsheet URL: {args.spreadsheet_url}")
        print(f"Username: {args.username}")
        print("Password: [hidden]")
        
        manager = CalendarManager(
            api_url=args.api_url,
            spreadsheet_url=args.spreadsheet_url,
            username=args.username,
            password=args.password
        )
        manager.run()
    except Exception as e:
        logger.critical(f"Application crashed: {e}")
        exit(1)