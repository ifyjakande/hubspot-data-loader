#!/usr/bin/env python3
"""
HubSpot Data Loader - Generates and loads realistic sample data into HubSpot
"""

import os
import random
import requests
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker for generating realistic data
fake = Faker()

# HubSpot API configuration - MUST be set via environment variable
HUBSPOT_API_KEY = os.environ.get('HUBSPOT_API_KEY')
if not HUBSPOT_API_KEY:
    raise ValueError("HUBSPOT_API_KEY environment variable is required")

HUBSPOT_API_URL = 'https://api.hubapi.com'

def create_contact(email, first_name, last_name, phone=None, job_title=None, company_name=None):
    """Create a contact in HubSpot"""
    url = f'{HUBSPOT_API_URL}/crm/v3/objects/contacts'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }

    properties = {
        'email': email,
        'firstname': first_name,
        'lastname': last_name
    }

    if phone:
        properties['phone'] = phone
    if job_title:
        properties['jobtitle'] = job_title
    if company_name:
        properties['company'] = company_name

    payload = {'properties': properties}

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 409:
            print(f"Contact {email} already exists, skipping...")
            return None
        else:
            print(f"Error creating contact: {e}")
            print(f"Response: {response.text}")
            return None

def create_company(name, domain=None, industry=None, city=None, country=None):
    """Create a company in HubSpot"""
    url = f'{HUBSPOT_API_URL}/crm/v3/objects/companies'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
        'Content-Type': 'application/json'
    }

    properties = {
        'name': name
    }

    if domain:
        properties['domain'] = domain
    if industry:
        properties['industry'] = industry
    if city:
        properties['city'] = city
    if country:
        properties['country'] = country

    payload = {'properties': properties}

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 409:
            print(f"Company {name} already exists, skipping...")
            return None
        else:
            print(f"Error creating company: {e}")
            print(f"Response: {response.text}")
            return None

def generate_sample_data(num_contacts=10, num_companies=5):
    """Generate and load sample data into HubSpot"""
    print(f"Starting data load at {datetime.now()}")
    print(f"Target: {num_contacts} contacts, {num_companies} companies")
    print("-" * 50)

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

    # Create companies
    companies_created = []
    print("\nCreating Companies...")
    for i in range(num_companies):
        company_name = fake.company()
        domain = company_name.lower().replace(' ', '').replace(',', '') + '.com'
        industry = random.choice(industries)
        city = fake.city()
        country = fake.country()

        result = create_company(company_name, domain, industry, city, country)
        if result:
            companies_created.append({'name': company_name, 'domain': domain})
            print(f"  [OK] Created: {company_name} ({industry}) - {city}, {country}")

    print(f"\nCompanies created: {len(companies_created)}/{num_companies}")

    # Create contacts
    contacts_created = []
    print("\nCreating Contacts...")
    for i in range(num_contacts):
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        # Use company domain for email to avoid HubSpot creating empty company records
        if companies_created:
            company = random.choice(companies_created)
            company_name = company['name']
            email = f"{first_name.lower()}.{last_name.lower()}@{company['domain']}"
        else:
            company_name = fake.company()
            email = f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}"
        
        phone = fake.phone_number()
        job_title = random.choice(job_titles)

        result = create_contact(email, first_name, last_name, phone, job_title, company_name)
        if result:
            contacts_created.append(email)
            print(f"  [OK] Created: {first_name} {last_name} ({job_title}) at {company_name}")

    print(f"\nContacts created: {len(contacts_created)}/{num_contacts}")
    print("-" * 50)
    print(f"Data load completed at {datetime.now()}")

if __name__ == '__main__':
    # Load smaller batches for incremental loading
    # You can adjust these numbers in GitHub Actions
    num_contacts = int(os.environ.get('NUM_CONTACTS', '5'))
    num_companies = int(os.environ.get('NUM_COMPANIES', '3'))

    generate_sample_data(num_contacts, num_companies)
