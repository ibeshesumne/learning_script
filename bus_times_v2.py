import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime
from dateutil.parser import parse
import concurrent.futures

def run_bus_times_from_central():
    # Code for the "Bus times from Central" program
    df = pd.read_csv('bus_stop_data_complete_outbound.csv', dtype={'stop': str})

    st.sidebar.markdown("## 📍 **Choose start point:**")
    st.sidebar.markdown("   🏢 **Exchange Square - 001032**", unsafe_allow_html=True)
    st.sidebar.markdown("   🏛️ **City Hall - 001031**", unsafe_allow_html=True)
    st.sidebar.markdown("   🏛️ **Admiralty - 001136**", unsafe_allow_html=True)

    #start_stop = st.sidebar.text_input("Enter number e.g. 001032: ")  # User inputs the start stop
    #start_stop = st.sidebar.text_input("Start Point", "Enter number", label_visibility='hidden')
    st.sidebar.markdown("*Enter number*", unsafe_allow_html=True)
    start_stop = st.sidebar.text_input("Start Point", "Enter number", label_visibility='hidden')


    if start_stop:  # Check if start_stop is not empty
        end_stops = ['002378', '002302', '002277', '002400']

        # Filter DataFrame for start and end stops
        start_df = df[df['stop'] == start_stop]
        end_df = df[df['stop'].isin(end_stops)]

        # Find common routes between start and end stops
        common_routes = pd.Series(list(set(start_df['route']).intersection(set(end_df['route']))), dtype=object)

        def fetch_eta(url):
            response = requests.get(url, timeout=20)
            if response.status_code == 200:
                return response.json()['data']
            else:
                return []

        all_data = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for route in common_routes:
                start_url = f"https://rt.data.gov.hk/v2/transport/citybus/eta/CTB/{start_stop}/{route}"
                futures.append(executor.submit(fetch_eta, start_url))
                for end_stop in end_stops:
                    end_url = f"https://rt.data.gov.hk/v2/transport/citybus/eta/CTB/{end_stop}/{route}"
                    futures.append(executor.submit(fetch_eta, end_url))
            for future in concurrent.futures.as_completed(futures):
                all_data.extend(future.result())

        # Convert the 'eta' field to datetime for all data
        for item in all_data:
            item['eta'] = parse(item['eta'])

        # Check if all_data is not empty
        if all_data:
            # Create a DataFrame from the collected data
            df = pd.DataFrame(all_data)

            # Keep only the specified columns
            df = df[['route', 'stop', 'dest_en', 'eta']]

            # Sort the DataFrame by 'eta'
            df = df.sort_values(by='eta')

            # Exclude '001032' stop from the DataFrame
            df = df.query('dest_en != "Central (Exchange Square)"')

            # Display the sorted DataFrame
            st.dataframe(df)
        else:
            st.write("No data available for the given start stop.")

    # Source and citation box
    st.markdown("---")
    st.markdown("## Source and Citation")
    st.markdown("The data is obtained by application program interface from Citybus Limited with link from [Hong Kong government](https://data.gov.hk/en-data/dataset/ctb-eta-transport-realtime-eta/resource/e1961565-f6ba-4831-958e-1b2dab7b8703).")
    st.markdown("The presented table provides estimated time for next bus departures and no responsibility assumed for those times.")

def run_bus_times_to_central():
    # Code for the "Bus times to Central" program
    # Select bus stop
    bus_stop = {
        'HKCC - 002349': '002349',
        'Ocean Park carpark - 002353': '002353'
    }

    # User input for bus stop
    selected_route = st.sidebar.selectbox('Select bus stop:', list(bus_stop.keys()))
    stop = bus_stop[selected_route]

    # Read the CSV file
    df = pd.read_csv('bus_stop_data_complete_inbound.csv', dtype={'stop': str})

    # Filter the DataFrame based on the user input
    filtered_df = df[df['stop'] == stop]

    # Get the unique routes for the bus stop
    routes = filtered_df['route'].unique()

    # Fetch the data for each route
    def fetch_data(route):
        url = f"https://rt.data.gov.hk/v2/transport/citybus/eta/CTB/{stop}/{route}"
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()['data']

    all_data = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_data, route) for route in routes]
        for future in futures:
            all_data.extend(future.result())

    # Convert the 'eta' field to datetime for all data
    for item in all_data:
        item['eta'] = parse(item['eta'])

    # Create a DataFrame from the collected data
    df = pd.DataFrame(all_data)[['route', 'stop', 'dest_en', 'eta']]

    # Sort the DataFrame by 'eta'
    df = df.sort_values(by='eta')

    # Display the DataFrame
    st.dataframe(df)

    # Source and citation box
    st.markdown("---")
    st.markdown("## Source and Citation")
    st.markdown("The data is obtained by application program interface from Citybus Limited with link from [Hong Kong government](https://data.gov.hk/en-data/dataset/ctb-eta-transport-realtime-eta/resource/e1961565-f6ba-4831-958e-1b2dab7b8703).")
    st.markdown("The presented table provides estimated time for next bus departures and no responsibility assumed for those times.")

st.title("Bus Times to and from Central")

with st.sidebar.expander("🚍 **Select Program**", expanded=True):
    menu_options = {
        "Bus times from Central": run_bus_times_from_central,
        "Bus times to Central": run_bus_times_to_central,
    }

    #selected_option = st.selectbox("", list(menu_options.keys()))
    selected_option = st.selectbox("Select an option", list(menu_options.keys()), label_visibility='hidden')

menu_options[selected_option]()