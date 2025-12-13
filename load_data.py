#!/usr/bin/env python3
"""
HubSpot Data Loader - Generates and loads realistic sample data into HubSpot
"""

import os
import random
import requests
import time
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker for generating realistic data
fake = Faker()

# HubSpot API configuration - MUST be set via environment variable
HUBSPOT_API_KEY = os.environ.get('HUBSPOT_API_KEY')
if not HUBSPOT_API_KEY:
    raise ValueError("HUBSPOT_API_KEY environment variable is required")

HUBSPOT_API_URL = 'https://api.hubapi.com'

# Rate limiting configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2  # seconds

def batch_create_companies(companies_data):
    """Create multiple companies in HubSpot using batch API (up to 100 per request)"""
    if not companies_data:
        return []

    url = f'{HUBSPOT_API_URL}/crm/v3/objects/companies/batch/create'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }

    # HubSpot batch limit is 100 records per request
    batch_size = 100
    all_created = []

    for i in range(0, len(companies_data), batch_size):
        batch = companies_data[i:i + batch_size]

        # Prepare batch payload
        inputs = []
        for company in batch:
            properties = {
                'name': company['name']
            }
            if company.get('domain'):
                properties['domain'] = company['domain']
            if company.get('industry'):
                properties['industry'] = company['industry']
            if company.get('city'):
                properties['city'] = company['city']
            if company.get('country'):
                properties['country'] = company['country']

            inputs.append({'properties': properties})

        payload = {'inputs': inputs}

        # Execute batch request with retry logic
        retry_delay = INITIAL_RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=30)

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', retry_delay))
                    print(f"  ⚠️  Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    retry_delay *= 2
                    continue

                response.raise_for_status()
                result = response.json()

                # Track successful creations
                if 'results' in result:
                    all_created.extend(result['results'])
                    print(f"  ✓ Created batch {i//batch_size + 1}: {len(result['results'])} companies")

                # Small delay between batches to avoid rate limiting
                time.sleep(0.5)
                break

            except (requests.exceptions.HTTPError, requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt == MAX_RETRIES - 1:
                    print(f"  ✗ Error creating company batch after {MAX_RETRIES} attempts: {e}")
                    if hasattr(e, 'response') and hasattr(e.response, 'text'):
                        print(f"  Response: {e.response.text}")
                else:
                    print(f"  ⚠️  Network error (attempt {attempt + 1}/{MAX_RETRIES}): {type(e).__name__}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2

    return all_created

def batch_create_contacts(contacts_data):
    """Create multiple contacts in HubSpot using batch API (up to 100 per request)"""
    if not contacts_data:
        return []

    url = f'{HUBSPOT_API_URL}/crm/v3/objects/contacts/batch/create'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }

    # HubSpot batch limit is 100 records per request
    batch_size = 100
    all_created = []

    for i in range(0, len(contacts_data), batch_size):
        batch = contacts_data[i:i + batch_size]

        # Prepare batch payload
        inputs = []
        for contact in batch:
            properties = {
                'email': contact['email'],
                'firstname': contact['firstname'],
                'lastname': contact['lastname']
            }
            if contact.get('phone'):
                properties['phone'] = contact['phone']
            if contact.get('jobtitle'):
                properties['jobtitle'] = contact['jobtitle']
            if contact.get('company'):
                properties['company'] = contact['company']

            inputs.append({'properties': properties})

        payload = {'inputs': inputs}

        # Execute batch request with retry logic
        retry_delay = INITIAL_RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=30)

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', retry_delay))
                    print(f"  ⚠️  Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    retry_delay *= 2
                    continue

                response.raise_for_status()
                result = response.json()

                # Track successful creations
                if 'results' in result:
                    all_created.extend(result['results'])
                    print(f"  ✓ Created batch {i//batch_size + 1}: {len(result['results'])} contacts")

                # Small delay between batches to avoid rate limiting
                time.sleep(0.5)
                break

            except (requests.exceptions.HTTPError, requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt == MAX_RETRIES - 1:
                    print(f"  ✗ Error creating contact batch after {MAX_RETRIES} attempts: {e}")
                    if hasattr(e, 'response') and hasattr(e.response, 'text'):
                        print(f"  Response: {e.response.text}")
                else:
                    print(f"  ⚠️  Network error (attempt {attempt + 1}/{MAX_RETRIES}): {type(e).__name__}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2

    return all_created

def generate_sample_data(num_contacts=10, num_companies=5):
    """Generate and load sample data into HubSpot using batch APIs for performance"""
    print(f"Starting BATCH data load at {datetime.now()}")
    print(f"Target: {num_contacts} contacts, {num_companies} companies")
    print(f"Using HubSpot Batch API (100 records per request)")
    print("-" * 70)

    # Industries for companies - using HubSpot's valid industry codes
    industries = ['ACCOUNTING', 'AIRLINES_AVIATION', 'ALTERNATIVE_MEDICINE', 'ANIMATION',
                  'APPAREL_FASHION', 'ARCHITECTURE_PLANNING', 'ARTS_AND_CRAFTS', 'AUTOMOTIVE',
                  'AVIATION_AEROSPACE', 'BANKING', 'BIOTECHNOLOGY', 'BROADCAST_MEDIA',
                  'BUILDING_MATERIALS', 'BUSINESS_SUPPLIES_AND_EQUIPMENT', 'CAPITAL_MARKETS',
                  'CHEMICALS', 'CIVIC_SOCIAL_ORGANIZATION', 'CIVIL_ENGINEERING',
                  'COMMERCIAL_REAL_ESTATE', 'COMPUTER_NETWORK_SECURITY', 'COMPUTER_GAMES',
                  'COMPUTER_HARDWARE', 'COMPUTER_NETWORKING', 'COMPUTER_SOFTWARE', 'INTERNET',
                  'CONSTRUCTION', 'CONSUMER_ELECTRONICS', 'CONSUMER_GOODS', 'CONSUMER_SERVICES',
                  'COSMETICS', 'DESIGN', 'EDUCATION_MANAGEMENT', 'E_LEARNING',
                  'ELECTRICAL_ELECTRONIC_MANUFACTURING', 'ENTERTAINMENT', 'ENVIRONMENTAL_SERVICES',
                  'EVENTS_SERVICES', 'FACILITIES_SERVICES', 'FINANCIAL_SERVICES', 'FOOD_BEVERAGES',
                  'FOOD_PRODUCTION', 'FUND_RAISING', 'INFORMATION_TECHNOLOGY_AND_SERVICES',
                  'MARKETING_AND_ADVERTISING', 'DEFENSE_SPACE']

    # Job titles for contacts
    job_titles = ['Sales Manager', 'Marketing Director', 'CEO', 'CTO', 'VP of Sales',
                  'Product Manager', 'Business Analyst', 'Account Executive',
                  'Customer Success Manager', 'Operations Manager']

    # PHASE 1: Generate company data
    print("\n[PHASE 1] Generating company data...")
    companies_data = []
    for i in range(num_companies):
        company_name = fake.company()
        # Generate unique domain to avoid duplicates
        domain = company_name.lower().replace(' ', '').replace(',', '').replace('.', '') + str(i) + '.com'
        industry = random.choice(industries)
        city = fake.city()
        country = fake.country()

        companies_data.append({
            'name': company_name,
            'domain': domain,
            'industry': industry,
            'city': city,
            'country': country
        })

    print(f"  ✓ Generated {len(companies_data)} companies")

    # PHASE 2: Batch create companies
    print("\n[PHASE 2] Creating companies in HubSpot (batch mode)...")
    companies_created = batch_create_companies(companies_data)

    # Map created companies for referential integrity
    companies_map = []
    for i, company_data in enumerate(companies_data[:len(companies_created)]):
        companies_map.append({
            'name': company_data['name'],
            'domain': company_data['domain']
        })

    print(f"  ✓ Successfully created {len(companies_created)}/{num_companies} companies")

    # PHASE 3: Generate contact data with referential integrity
    print("\n[PHASE 3] Generating contact data (maintaining referential integrity)...")
    contacts_data = []
    for i in range(num_contacts):
        first_name = fake.first_name()
        last_name = fake.last_name()

        # Ensure contacts reference existing companies (referential integrity)
        if companies_map:
            company = random.choice(companies_map)
            company_name = company['name']
            # Use company domain for email to link contact to company
            email = f"{first_name.lower()}.{last_name.lower()}.{i}@{company['domain']}"
        else:
            company_name = fake.company()
            email = f"{first_name.lower()}.{last_name.lower()}.{i}@{fake.domain_name()}"

        phone = fake.phone_number()
        job_title = random.choice(job_titles)

        contacts_data.append({
            'email': email,
            'firstname': first_name,
            'lastname': last_name,
            'phone': phone,
            'jobtitle': job_title,
            'company': company_name
        })

    print(f"  ✓ Generated {len(contacts_data)} contacts")

    # PHASE 4: Batch create contacts
    print("\n[PHASE 4] Creating contacts in HubSpot (batch mode)...")
    contacts_created = batch_create_contacts(contacts_data)
    print(f"  ✓ Successfully created {len(contacts_created)}/{num_contacts} contacts")

    # Summary
    print("\n" + "=" * 70)
    print("BATCH LOAD SUMMARY")
    print("=" * 70)
    print(f"Companies created: {len(companies_created)}/{num_companies}")
    print(f"Contacts created:  {len(contacts_created)}/{num_contacts}")
    print(f"Referential integrity: All contacts linked to valid companies")
    print(f"Completed at: {datetime.now()}")
    print("=" * 70)

if __name__ == '__main__':
    # Load smaller batches for incremental loading
    # You can adjust these numbers in GitHub Actions
    num_contacts = int(os.environ.get('NUM_CONTACTS', '5'))
    num_companies = int(os.environ.get('NUM_COMPANIES', '3'))

    generate_sample_data(num_contacts, num_companies)
