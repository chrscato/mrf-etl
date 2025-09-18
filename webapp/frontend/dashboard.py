"""
Streamlit Dashboard for MRF Data Lookup Tool
Interactive web interface for visualizing MRF ETL data
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime
import time

# Page configuration
st.set_page_config(
    page_title="MRF Data Lookup Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = "http://localhost:8000"

# Helper functions
@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_data(endpoint, params=None):
    """Fetch data from API with caching"""
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data: {e}")
        return None

def format_currency(value):
    """Format value as currency"""
    return f"${value:,.2f}" if value else "$0.00"

def format_number(value):
    """Format value as number with commas"""
    return f"{value:,}" if value else "0"

# Main dashboard
def main():
    st.title("📊 MRF Data Lookup Dashboard")
    st.markdown("Interactive visualization of Machine Readable Files (MRF) data")
    
    # Sidebar for filters
    st.sidebar.header("🔍 Filters")
    
    # Check API health
    with st.spinner("Checking API connection..."):
        health_data = fetch_data("/api/health")
    
    if not health_data:
        st.error("❌ Cannot connect to API. Please ensure the backend is running.")
        st.stop()
    
    if health_data.get("status") != "healthy":
        st.error("❌ API is not healthy. Please check the backend.")
        st.stop()
    
    st.success("✅ API connection successful")
    
    # Get available data
    available_data = fetch_data("/api/meta/available-data")
    if not available_data:
        st.error("❌ Cannot fetch available data")
        st.stop()
    
    # Filter controls
    data_availability = available_data.get("data_availability", [])
    available_payers = available_data.get("available_payers", [])
    
    if not data_availability:
        st.warning("⚠️ No data available")
        st.stop()
    
    # State and year_month selection
    states = sorted(list(set([item["state"] for item in data_availability])))
    selected_state = st.sidebar.selectbox("Select State", states)
    
    # Filter year_months by selected state
    state_year_months = [
        item["year_month"] for item in data_availability 
        if item["state"] == selected_state
    ]
    selected_year_month = st.sidebar.selectbox("Select Year-Month", state_year_months)
    
    # Payer filter
    payer_options = ["All"] + available_payers
    selected_payer = st.sidebar.selectbox("Filter by Payer", payer_options)
    payer_filter = None if selected_payer == "All" else selected_payer
    
    # Code type filter
    code_types = ["All", "CPT", "HCPCS", "REV"]
    selected_code_type = st.sidebar.selectbox("Filter by Code Type", code_types)
    code_type_filter = None if selected_code_type == "All" else selected_code_type
    
    # Advanced filters section
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔧 Advanced Filters")
    
    # Billing class filter
    billing_classes = ["All", "professional", "institutional"]
    selected_billing_class = st.sidebar.selectbox("Filter by Billing Class", billing_classes)
    billing_class_filter = None if selected_billing_class == "All" else selected_billing_class
    
    # TIN value filter
    st.sidebar.markdown("**TIN Value Filter**")
    tin_search = st.sidebar.text_input("Search TIN values", placeholder="Enter TIN to search")
    tin_value_filter = None
    
    if tin_search:
        # Get available TIN values for the selected state/year_month
        tin_data = fetch_data("/api/meta/dimension-values", {
            "state": selected_state,
            "year_month": selected_year_month,
            "dimension": "tin_value"
        })
        
        if tin_data and tin_data["values"]:
            # Filter TIN values based on search
            matching_tins = [
                item["value"] for item in tin_data["values"] 
                if tin_search.lower() in item["value"].lower()
            ]
            
            if matching_tins:
                selected_tin = st.sidebar.selectbox(
                    "Select TIN Value", 
                    ["None"] + matching_tins[:20]  # Limit to first 20 matches
                )
                tin_value_filter = None if selected_tin == "None" else selected_tin
            else:
                st.sidebar.info("No matching TIN values found")
    
    # Negotiated type filter
    negotiated_types = ["All", "negotiated", "derived", "fee schedule"]
    selected_negotiated_type = st.sidebar.selectbox("Filter by Negotiated Type", negotiated_types)
    negotiated_type_filter = None if selected_negotiated_type == "All" else selected_negotiated_type
    
    # Negotiation arrangement filter
    negotiation_arrangements = ["All", "ffs", "capitation", "other"]
    selected_negotiation_arrangement = st.sidebar.selectbox("Filter by Negotiation Arrangement", negotiation_arrangements)
    negotiation_arrangement_filter = None if selected_negotiation_arrangement == "All" else selected_negotiation_arrangement
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "📈 Overview", 
        "🏥 By Payer", 
        "🔬 By Procedure", 
        "📋 Rate Details", 
        "👨‍⚕️ Provider Search",
        "🔍 Dimension Explorer",
        "⚡ Fast Search",
        "🧭 Data Explorer"
    ])
    
    with tab1:
        st.header("📈 Data Overview")
        
        # Get summary data
        summary_params = {
            "state": selected_state,
            "year_month": selected_year_month
        }
        if payer_filter:
            summary_params["payer"] = payer_filter
        if code_type_filter:
            summary_params["code_type"] = code_type_filter
        if billing_class_filter:
            summary_params["billing_class"] = billing_class_filter
        if tin_value_filter:
            summary_params["tin_value"] = tin_value_filter
        if negotiated_type_filter:
            summary_params["negotiated_type"] = negotiated_type_filter
        if negotiation_arrangement_filter:
            summary_params["negotiation_arrangement"] = negotiation_arrangement_filter
        
        summary_data = fetch_data("/api/rates/summary", summary_params)
        
        if summary_data:
            summary = summary_data["summary"]
            
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="Total Rates",
                    value=format_number(summary["total_rates"]),
                    help="Total number of negotiated rates"
                )
            
            with col2:
                st.metric(
                    label="Average Rate",
                    value=format_currency(summary["avg_rate"]),
                    help="Average negotiated rate"
                )
            
            with col3:
                st.metric(
                    label="Median Rate",
                    value=format_currency(summary["median_rate"]),
                    help="Median negotiated rate"
                )
            
            with col4:
                st.metric(
                    label="Rate Range",
                    value=f"{format_currency(summary['min_rate'])} - {format_currency(summary['max_rate'])}",
                    help="Range from minimum to maximum rate"
                )
            
            # Additional metrics
            col5, col6, col7 = st.columns(3)
            
            with col5:
                st.metric(
                    label="Unique Procedures",
                    value=format_number(summary["unique_procedures"]),
                    help="Number of unique procedure codes"
                )
            
            with col6:
                st.metric(
                    label="Unique Payers",
                    value=format_number(summary["unique_payers"]),
                    help="Number of unique payers"
                )
            
            with col7:
                if summary["total_rates"] > 0:
                    rate_per_procedure = summary["total_rates"] / summary["unique_procedures"]
                    st.metric(
                        label="Avg Rates per Procedure",
                        value=f"{rate_per_procedure:.1f}",
                        help="Average number of rates per procedure"
                    )
    
    with tab2:
        st.header("🏥 Analysis by Payer")
        
        # Get payer data
        payer_data = fetch_data("/api/rates/by-payer", {
            "state": selected_state,
            "year_month": selected_year_month,
            "limit": 20
        })
        
        if payer_data and payer_data["payers"]:
            df_payers = pd.DataFrame(payer_data["payers"])
            
            # Payer metrics
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Rate Count by Payer")
                fig = px.bar(
                    df_payers.head(10),
                    x="payer_name",
                    y="rate_count",
                    title="Top 10 Payers by Rate Count",
                    labels={"payer_name": "Payer", "rate_count": "Number of Rates"}
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Average Rate by Payer")
                fig = px.bar(
                    df_payers.head(10),
                    x="payer_name",
                    y="avg_rate",
                    title="Top 10 Payers by Average Rate",
                    labels={"payer_name": "Payer", "avg_rate": "Average Rate ($)"}
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            # Detailed payer table
            st.subheader("Detailed Payer Statistics")
            st.dataframe(
                df_payers,
                use_container_width=True,
                column_config={
                    "payer_name": "Payer Name",
                    "rate_count": "Rate Count",
                    "avg_rate": st.column_config.NumberColumn("Avg Rate", format="$%.2f"),
                    "min_rate": st.column_config.NumberColumn("Min Rate", format="$%.2f"),
                    "max_rate": st.column_config.NumberColumn("Max Rate", format="$%.2f"),
                    "median_rate": st.column_config.NumberColumn("Median Rate", format="$%.2f"),
                    "unique_procedures": "Unique Procedures"
                }
            )
    
    with tab3:
        st.header("🔬 Analysis by Procedure")
        
        # Get procedure data
        procedure_params = {
            "state": selected_state,
            "year_month": selected_year_month,
            "limit": 20
        }
        if code_type_filter:
            procedure_params["code_type"] = code_type_filter
        if billing_class_filter:
            procedure_params["billing_class"] = billing_class_filter
        if tin_value_filter:
            procedure_params["tin_value"] = tin_value_filter
        
        procedure_data = fetch_data("/api/rates/by-procedure", procedure_params)
        
        if procedure_data and procedure_data["procedures"]:
            df_procedures = pd.DataFrame(procedure_data["procedures"])
            
            # Procedure metrics
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Rate Count by Procedure")
                fig = px.bar(
                    df_procedures.head(10),
                    x="code",
                    y="rate_count",
                    title="Top 10 Procedures by Rate Count",
                    labels={"code": "Procedure Code", "rate_count": "Number of Rates"},
                    hover_data=["code_desc"]
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Average Rate by Procedure")
                fig = px.bar(
                    df_procedures.head(10),
                    x="code",
                    y="avg_rate",
                    title="Top 10 Procedures by Average Rate",
                    labels={"code": "Procedure Code", "avg_rate": "Average Rate ($)"},
                    hover_data=["code_desc"]
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            # Rate distribution for top procedure
            if len(df_procedures) > 0:
                st.subheader("Rate Distribution Analysis")
                top_procedure = df_procedures.iloc[0]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Top Procedure", f"{top_procedure['code']} - {top_procedure['code_desc']}")
                    st.metric("Rate Count", format_number(top_procedure['rate_count']))
                    st.metric("Unique Payers", format_number(top_procedure['unique_payers']))
                
                with col2:
                    st.metric("Min Rate", format_currency(top_procedure['min_rate']))
                    st.metric("Max Rate", format_currency(top_procedure['max_rate']))
                    st.metric("Rate Range", format_currency(top_procedure['max_rate'] - top_procedure['min_rate']))
            
            # Detailed procedure table
            st.subheader("Detailed Procedure Statistics")
            st.dataframe(
                df_procedures,
                use_container_width=True,
                column_config={
                    "code_type": "Code Type",
                    "code": "Code",
                    "code_desc": "Description",
                    "rate_count": "Rate Count",
                    "avg_rate": st.column_config.NumberColumn("Avg Rate", format="$%.2f"),
                    "min_rate": st.column_config.NumberColumn("Min Rate", format="$%.2f"),
                    "max_rate": st.column_config.NumberColumn("Max Rate", format="$%.2f"),
                    "median_rate": st.column_config.NumberColumn("Median Rate", format="$%.2f"),
                    "unique_payers": "Unique Payers"
                }
            )
    
    with tab4:
        st.header("📋 Rate Details")
        
        # Additional filters for detailed view
        col1, col2 = st.columns(2)
        
        with col1:
            procedure_code = st.text_input("Filter by Procedure Code (optional)", placeholder="e.g., 10121")
        
        with col2:
            limit = st.slider("Number of records to show", 10, 500, 100)
        
        # Get detailed rate data
        detail_params = {
            "state": selected_state,
            "year_month": selected_year_month,
            "limit": limit
        }
        if payer_filter:
            detail_params["payer"] = payer_filter
        if procedure_code:
            detail_params["code"] = procedure_code
        if billing_class_filter:
            detail_params["billing_class"] = billing_class_filter
        if tin_value_filter:
            detail_params["tin_value"] = tin_value_filter
        
        detail_data = fetch_data("/api/rates/detail", detail_params)
        
        if detail_data and detail_data["records"]:
            df_details = pd.DataFrame(detail_data["records"])
            
            st.subheader(f"Rate Details ({len(df_details)} records)")
            
            # Summary stats for filtered data
            if len(df_details) > 0:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Records", format_number(len(df_details)))
                
                with col2:
                    avg_rate = df_details['negotiated_rate'].mean()
                    st.metric("Avg Rate", format_currency(avg_rate))
                
                with col3:
                    min_rate = df_details['negotiated_rate'].min()
                    st.metric("Min Rate", format_currency(min_rate))
                
                with col4:
                    max_rate = df_details['negotiated_rate'].max()
                    st.metric("Max Rate", format_currency(max_rate))
            
            # Rate distribution chart
            if len(df_details) > 1:
                st.subheader("Rate Distribution")
                fig = px.histogram(
                    df_details,
                    x="negotiated_rate",
                    nbins=20,
                    title="Distribution of Negotiated Rates",
                    labels={"negotiated_rate": "Rate ($)", "count": "Frequency"}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Detailed records table
            st.subheader("Detailed Records")
            st.dataframe(
                df_details,
                use_container_width=True,
                column_config={
                    "payer_name": "Payer",
                    "code_type": "Code Type",
                    "code": "Code",
                    "code_desc": "Description",
                    "negotiated_rate": st.column_config.NumberColumn("Rate", format="$%.2f"),
                    "negotiated_type": "Type",
                    "negotiation_arrangement": "Arrangement",
                    "expiration_date": "Expiration"
                }
            )
    
    with tab5:
        st.header("👨‍⚕️ Provider Search")
        
        # Provider search
        search_query = st.text_input("Search for providers", placeholder="Enter organization name, first name, or last name")
        
        if search_query:
            provider_data = fetch_data("/api/providers/search", {"q": search_query, "limit": 50})
            
            if provider_data and provider_data["providers"]:
                df_providers = pd.DataFrame(provider_data["providers"])
                
                st.subheader(f"Search Results ({len(df_providers)} providers found)")
                
                # Provider type distribution
                if len(df_providers) > 0:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("Provider Type Distribution")
                        type_counts = df_providers['enumeration_type'].value_counts()
                        fig = px.pie(
                            values=type_counts.values,
                            names=type_counts.index,
                            title="Provider Types"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        st.subheader("Status Distribution")
                        status_counts = df_providers['status'].value_counts()
                        fig = px.pie(
                            values=status_counts.values,
                            names=status_counts.index,
                            title="Provider Status"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                # Provider details table
                st.subheader("Provider Details")
                st.dataframe(
                    df_providers,
                    use_container_width=True,
                    column_config={
                        "npi": "NPI",
                        "organization_name": "Organization",
                        "first_name": "First Name",
                        "last_name": "Last Name",
                        "enumeration_type": "Type",
                        "primary_taxonomy_desc": "Specialty",
                        "status": "Status"
                    }
                )
            else:
                st.info("No providers found matching your search criteria.")
        else:
            st.info("Enter a search term to find providers.")
    
    with tab6:
        st.header("🔍 Dimension Explorer")
        st.markdown("Explore available values for each dimension to understand your data better.")
        
        # Dimension selection
        dimension = st.selectbox(
            "Select Dimension to Explore",
            ["billing_class", "code_type", "negotiated_type", "negotiation_arrangement", "tin_value"]
        )
        
        # Get dimension values
        dimension_data = fetch_data("/api/meta/dimension-values", {
            "state": selected_state,
            "year_month": selected_year_month,
            "dimension": dimension
        })
        
        if dimension_data and dimension_data["values"]:
            df_dimension = pd.DataFrame(dimension_data["values"])
            
            st.subheader(f"Available {dimension.replace('_', ' ').title()} Values")
            
            # Show summary stats
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Values", format_number(len(df_dimension)))
            
            with col2:
                total_count = df_dimension['count'].sum()
                st.metric("Total Records", format_number(total_count))
            
            with col3:
                avg_per_value = total_count / len(df_dimension) if len(df_dimension) > 0 else 0
                st.metric("Avg Records per Value", f"{avg_per_value:.1f}")
            
            # Top values chart
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Top 10 Values by Count")
                top_values = df_dimension.head(10)
                fig = px.bar(
                    top_values,
                    x="value",
                    y="count",
                    title=f"Top 10 {dimension.replace('_', ' ').title()} Values",
                    labels={"value": dimension.replace('_', ' ').title(), "count": "Record Count"}
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Distribution")
                fig = px.pie(
                    top_values,
                    values="count",
                    names="value",
                    title=f"Distribution of Top 10 {dimension.replace('_', ' ').title()} Values"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Search functionality
            st.subheader("Search Values")
            search_term = st.text_input(f"Search {dimension.replace('_', ' ')} values", placeholder="Enter search term")
            
            if search_term:
                filtered_df = df_dimension[
                    df_dimension['value'].str.contains(search_term, case=False, na=False)
                ]
                
                if len(filtered_df) > 0:
                    st.write(f"Found {len(filtered_df)} matching values:")
                    st.dataframe(
                        filtered_df,
                        use_container_width=True,
                        column_config={
                            "value": dimension.replace('_', ' ').title(),
                            "count": "Record Count"
                        }
                    )
                else:
                    st.info("No matching values found.")
            
            # Full data table
            st.subheader("All Values")
            st.dataframe(
                df_dimension,
                use_container_width=True,
                column_config={
                    "value": dimension.replace('_', ' ').title(),
                    "count": "Record Count"
                }
            )
        else:
            st.warning(f"No data available for {dimension} in the selected state and time period.")
    
    with tab7:
        st.header("⚡ Fast Search - Optimized Indexed Queries")
        st.markdown("High-performance search using materialized views and indexing for instant results.")
        
        # Search type selection
        search_type = st.selectbox(
            "Select Search Type",
            ["Multi-Field Search", "TIN Search", "Organization Search", "Taxonomy Search", 
             "Procedure Category Search", "Billing Code Search", "Payer Search"],
            key="fast_search_type"
        )
        
        # Search form based on type
        if search_type == "Multi-Field Search":
            st.subheader("🔍 Multi-Field Search")
            st.markdown("Search across multiple fields simultaneously for comprehensive results.")
            
            # Provider filters
            st.markdown("### 👨‍⚕️ Provider Filters")
            col1, col2 = st.columns(2)
            
            with col1:
                primary_taxonomy_desc = st.text_input("Primary Taxonomy Description", placeholder="e.g., Internal Medicine")
                organization_name = st.text_input("Organization Name (Partial Match)", placeholder="e.g., Medical Group")
                npi = st.text_input("NPI", placeholder="e.g., 1234567890")
            
            with col2:
                enumeration_type = st.selectbox("Enumeration Type", ["", "1", "2"], 
                                              format_func=lambda x: {"": "All", "1": "Individual", "2": "Organization"}[x])
                billing_class = st.text_input("Billing Class", placeholder="e.g., professional")
            
            # Procedure filters
            st.markdown("### 🏥 Procedure Filters")
            col3, col4 = st.columns(2)
            
            with col3:
                proc_set = st.text_input("Procedure Set", placeholder="e.g., OFFICE_VISITS")
                proc_class = st.text_input("Procedure Class", placeholder="e.g., EVALUATION_MANAGEMENT")
            
            with col4:
                proc_group = st.text_input("Procedure Group", placeholder="e.g., PRIMARY_CARE")
                billing_code = st.text_input("Billing Code", placeholder="e.g., 99213")
            
            # TIN and Payer filters
            st.markdown("### 🏢 TIN & Payer Filters")
            col5, col6 = st.columns(2)
            
            with col5:
                tin_value = st.text_input("TIN Value", placeholder="e.g., 123456789")
            
            with col6:
                payer = st.text_input("Payer Name", placeholder="e.g., Aetna Life Insurance")
            
            search_button = st.button("🔍 Search", key="multi_field_search")
            
            if search_button:
                with st.spinner("Searching with optimized queries..."):
                    search_params = {
                        "state": selected_state,
                        "year_month": selected_year_month,
                        "limit": 200
                    }
                    
                    # Add non-empty filters
                    if primary_taxonomy_desc:
                        search_params["primary_taxonomy_desc"] = primary_taxonomy_desc
                    if organization_name:
                        search_params["organization_name"] = organization_name
                    if npi:
                        search_params["npi"] = npi
                    if enumeration_type:
                        search_params["enumeration_type"] = enumeration_type
                    if billing_class:
                        search_params["billing_class"] = billing_class
                    if proc_set:
                        search_params["proc_set"] = proc_set
                    if proc_class:
                        search_params["proc_class"] = proc_class
                    if proc_group:
                        search_params["proc_group"] = proc_group
                    if billing_code:
                        search_params["billing_code"] = billing_code
                    if tin_value:
                        search_params["tin_value"] = tin_value
                    if payer:
                        search_params["payer"] = payer
                    
                    results = fetch_data("/api/search/multi-field", search_params)
                    
                    if results and results["results"]:
                        st.success(f"✅ Found {results['result_count']} results")
                        
                        # Display results
                        df_results = pd.DataFrame(results["results"])
                        
                        # Summary metrics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Results", len(df_results))
                        with col2:
                            if 'negotiated_rate' in df_results.columns:
                                avg_rate = df_results['negotiated_rate'].mean()
                                st.metric("Avg Rate", f"${avg_rate:.2f}")
                        with col3:
                            unique_orgs = df_results['organization_name'].nunique() if 'organization_name' in df_results.columns else 0
                            st.metric("Unique Organizations", unique_orgs)
                        with col4:
                            unique_codes = df_results['code'].nunique() if 'code' in df_results.columns else 0
                            st.metric("Unique Codes", unique_codes)
                        
                        # Results table
                        st.subheader("Search Results")
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info("No results found for the specified criteria.")
        
        elif search_type == "TIN Search":
            st.subheader("🏢 TIN Search")
            tin_value = st.text_input("Enter TIN Value", placeholder="e.g., 123456789")
            search_button = st.button("🔍 Search TIN", key="tin_search")
            
            if search_button and tin_value:
                with st.spinner("Searching TIN..."):
                    results = fetch_data("/api/search/tin", {
                        "tin_value": tin_value,
                        "state": selected_state,
                        "year_month": selected_year_month,
                        "limit": 100
                    })
                    
                    if results and results["results"]:
                        st.success(f"✅ Found {results['result_count']} results for TIN: {tin_value}")
                        df_results = pd.DataFrame(results["results"])
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info(f"No results found for TIN: {tin_value}")
        
        elif search_type == "Organization Search":
            st.subheader("🏥 Organization Search")
            org_name = st.text_input("Enter Organization Name", placeholder="e.g., Atlanta Medical")
            search_button = st.button("🔍 Search Organization", key="org_search")
            
            if search_button and org_name:
                with st.spinner("Searching organization..."):
                    results = fetch_data("/api/search/organization", {
                        "org_name": org_name,
                        "state": selected_state,
                        "year_month": selected_year_month,
                        "limit": 100
                    })
                    
                    if results and results["results"]:
                        st.success(f"✅ Found {results['result_count']} results for organization: {org_name}")
                        df_results = pd.DataFrame(results["results"])
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info(f"No results found for organization: {org_name}")
        
        elif search_type == "Taxonomy Search":
            st.subheader("🏥 Taxonomy Search")
            taxonomy_desc = st.text_input("Enter Taxonomy Description", placeholder="e.g., Internal Medicine")
            search_button = st.button("🔍 Search Taxonomy", key="taxonomy_search")
            
            if search_button and taxonomy_desc:
                with st.spinner("Searching taxonomy..."):
                    results = fetch_data("/api/search/taxonomy", {
                        "taxonomy_desc": taxonomy_desc,
                        "state": selected_state,
                        "year_month": selected_year_month,
                        "limit": 100
                    })
                    
                    if results and results["results"]:
                        st.success(f"✅ Found {results['result_count']} results for taxonomy: {taxonomy_desc}")
                        df_results = pd.DataFrame(results["results"])
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info(f"No results found for taxonomy: {taxonomy_desc}")
        
        elif search_type == "Procedure Category Search":
            st.subheader("🔬 Procedure Category Search")
            proc_class = st.text_input("Enter Procedure Class", placeholder="e.g., EVALUATION_MANAGEMENT")
            search_button = st.button("🔍 Search Procedure Category", key="proc_class_search")
            
            if search_button and proc_class:
                with st.spinner("Searching procedure category..."):
                    results = fetch_data("/api/search/procedure-category", {
                        "proc_class": proc_class,
                        "state": selected_state,
                        "year_month": selected_year_month,
                        "limit": 100
                    })
                    
                    if results and results["results"]:
                        st.success(f"✅ Found {results['result_count']} results for procedure class: {proc_class}")
                        df_results = pd.DataFrame(results["results"])
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info(f"No results found for procedure class: {proc_class}")
        
        elif search_type == "Billing Code Search":
            st.subheader("💰 Billing Code Search")
            billing_code = st.text_input("Enter Billing Code", placeholder="e.g., 99213")
            search_button = st.button("🔍 Search Billing Code", key="billing_code_search")
            
            if search_button and billing_code:
                with st.spinner("Searching billing code..."):
                    results = fetch_data("/api/search/billing-code", {
                        "billing_code": billing_code,
                        "state": selected_state,
                        "year_month": selected_year_month,
                        "limit": 100
                    })
                    
                    if results and results["results"]:
                        st.success(f"✅ Found {results['result_count']} results for billing code: {billing_code}")
                        df_results = pd.DataFrame(results["results"])
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info(f"No results found for billing code: {billing_code}")
        
        elif search_type == "Payer Search":
            st.subheader("🏦 Payer Search")
            payer_name = st.text_input("Enter Payer Name", placeholder="e.g., Aetna")
            search_button = st.button("🔍 Search Payer", key="payer_search")
            
            if search_button and payer_name:
                with st.spinner("Searching payer..."):
                    results = fetch_data("/api/search/payer", {
                        "payer_name": payer_name,
                        "state": selected_state,
                        "year_month": selected_year_month,
                        "limit": 100
                    })
                    
                    if results and results["results"]:
                        st.success(f"✅ Found {results['result_count']} results for payer: {payer_name}")
                        df_results = pd.DataFrame(results["results"])
                        st.dataframe(df_results, use_container_width=True)
                    else:
                        st.info(f"No results found for payer: {payer_name}")
        
        # Search statistics
        st.markdown("---")
        st.subheader("📊 Search Statistics")
        
        with st.spinner("Loading search statistics..."):
            stats = fetch_data("/api/search/statistics", {
                "state": selected_state,
                "year_month": selected_year_month
            })
            
            if stats and stats["statistics"]:
                stats_data = stats["statistics"]
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Unique Providers", format_number(stats_data["unique_providers"]))
                    st.metric("Unique Organizations", format_number(stats_data["unique_organizations"]))
                
                with col2:
                    st.metric("Unique Taxonomies", format_number(stats_data["unique_taxonomies"]))
                    st.metric("Unique Procedures", format_number(stats_data["unique_procedures"]))
                
                with col3:
                    st.metric("Procedure Classes", format_number(stats_data["unique_procedure_classes"]))
                    st.metric("Unique Payers", format_number(stats_data["unique_payers"]))
                
                with col4:
                    st.metric("Unique TINs", format_number(stats_data["unique_tins"]))
                    st.metric("Total Records", format_number(stats_data["total_records"]))
        
        # Performance info
        st.markdown("---")
        st.info("""
        **⚡ Performance Features:**
        - **Materialized Views**: Pre-computed joins for instant results
        - **Indexed Queries**: Optimized database indexes for fast lookups
        - **Smart Caching**: Results cached for 5 minutes to reduce load
        - **Debounced Search**: Prevents excessive API calls during typing
        - **Progressive Loading**: Large results loaded in chunks
        """)
    
    with tab8:
        st.header("🧭 Data Explorer - Discover What's Available")
        st.markdown("Explore your data to understand what's available before searching. Select a category to see what data exists and how much.")
        
        # Category selection
        col1, col2 = st.columns(2)
        
        with col1:
            explore_category = st.selectbox(
                "Select Category to Explore",
                ["", "payer", "organization", "taxonomy", "procedure_set", "procedure_class"],
                format_func=lambda x: {
                    "": "Select a category...",
                    "payer": "Payer (Insurance Companies)",
                    "organization": "Organization Name", 
                    "taxonomy": "Taxonomy Description",
                    "procedure_set": "Procedure Set",
                    "procedure_class": "Procedure Class"
                }[x],
                key="explore_category"
            )
        
        with col2:
            explore_limit = st.selectbox(
                "Results Limit",
                [10, 25, 50],
                index=1,
                key="explore_limit"
            )
        
        if explore_category:
            # Get category statistics first
            with st.spinner("Loading data overview..."):
                category_stats = fetch_data("/api/explore/category-stats", {
                    "state": selected_state,
                    "year_month": selected_year_month
                })
            
            if category_stats and category_stats.get("category_statistics"):
                stats = category_stats["category_statistics"]
                
                # Display overview metrics
                st.subheader("📊 Data Overview")
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric("Payers", format_number(stats["payer"]["count"]))
                with col2:
                    st.metric("Organizations", format_number(stats["organization"]["count"]))
                with col3:
                    st.metric("Taxonomies", format_number(stats["taxonomy"]["count"]))
                with col4:
                    st.metric("Procedure Sets", format_number(stats["procedure_set"]["count"]))
                with col5:
                    st.metric("Procedure Classes", format_number(stats["procedure_class"]["count"]))
                
                # Pagination controls
                col1, col2, col3 = st.columns([1, 2, 1])
                with col1:
                    if 'explore_page' not in st.session_state:
                        st.session_state.explore_page = 0
                    
                    if st.button("⬅️ Previous", disabled=st.session_state.explore_page <= 0):
                        st.session_state.explore_page -= 1
                        st.rerun()
                
                with col2:
                    st.write(f"Page {st.session_state.explore_page + 1}")
                
                with col3:
                    if st.button("Next ➡️"):
                        st.session_state.explore_page += 1
                        st.rerun()
                
                # Get exploration data with pagination
                offset = st.session_state.explore_page * explore_limit
                with st.spinner(f"Loading page {st.session_state.explore_page + 1} of {explore_category} data..."):
                    exploration_data = fetch_data("/api/explore/data-availability", {
                        "state": selected_state,
                        "year_month": selected_year_month,
                        "category": explore_category,
                        "limit": explore_limit,
                        "offset": offset
                    })
                
                if exploration_data and exploration_data.get("results"):
                    results = exploration_data["results"]
                    has_more = exploration_data.get("has_more", False)
                    
                    st.subheader(f"🔍 {explore_category.title()} Exploration Results")
                    st.write(f"Showing {len(results)} results (Page {st.session_state.explore_page + 1})")
                    
                    # Show if there are more results
                    if has_more:
                        st.info("💡 There are more results available. Use the Next button to see more.")
                    elif st.session_state.explore_page > 0:
                        st.success("✅ You've reached the end of the results.")
                    
                    # Create DataFrame for display
                    df_exploration = pd.DataFrame(results)
                    
                    # Display summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        total_records = df_exploration['record_count'].sum()
                        st.metric("Total Records", format_number(total_records))
                    with col2:
                        total_providers = df_exploration['unique_providers'].sum()
                        st.metric("Total Providers", format_number(total_providers))
                    with col3:
                        total_procedures = df_exploration['unique_procedures'].sum()
                        st.metric("Total Procedures", format_number(total_procedures))
                    with col4:
                        avg_rate = df_exploration['avg_rate'].mean()
                        st.metric("Avg Rate", format_currency(avg_rate))
                    
                    # Display results table
                    st.subheader("📋 Detailed Results")
                    
                    # Format the DataFrame for better display
                    display_df = df_exploration.copy()
                    display_df['avg_rate'] = display_df['avg_rate'].apply(lambda x: f"${x:.2f}")
                    display_df['min_rate'] = display_df['min_rate'].apply(lambda x: f"${x:.2f}")
                    display_df['max_rate'] = display_df['max_rate'].apply(lambda x: f"${x:.2f}")
                    display_df['record_count'] = display_df['record_count'].apply(lambda x: f"{x:,}")
                    display_df['unique_providers'] = display_df['unique_providers'].apply(lambda x: f"{x:,}")
                    display_df['unique_procedures'] = display_df['unique_procedures'].apply(lambda x: f"{x:,}")
                    
                    # Rename columns for display
                    display_df = display_df.rename(columns={
                        'value': 'Value',
                        'record_count': 'Records',
                        'unique_providers': 'Providers',
                        'unique_procedures': 'Procedures',
                        'avg_rate': 'Avg Rate',
                        'min_rate': 'Min Rate',
                        'max_rate': 'Max Rate'
                    })
                    
                    st.dataframe(
                        display_df[['Value', 'Records', 'Providers', 'Procedures', 'Avg Rate', 'Min Rate', 'Max Rate']],
                        use_container_width=True,
                        height=400
                    )
                    
                    # Drill down functionality
                    st.subheader("🔍 Drill Down Analysis")
                    st.markdown("Select a value to drill down and see related data:")
                    
                    # Create a selectbox for drill down
                    drill_down_value = st.selectbox(
                        f"Select {explore_category} to drill down:",
                        [""] + df_exploration['value'].tolist(),
                        key="drill_down_value"
                    )
                    
                    if drill_down_value:
                        # Determine available drill down categories
                        drill_categories = {
                            'payer': ['organization', 'taxonomy', 'procedure_set', 'procedure_class'],
                            'organization': ['payer', 'taxonomy', 'procedure_set', 'procedure_class'],
                            'taxonomy': ['payer', 'organization', 'procedure_set', 'procedure_class'],
                            'procedure_set': ['payer', 'organization', 'taxonomy', 'procedure_class'],
                            'procedure_class': ['payer', 'organization', 'taxonomy', 'procedure_set']
                        }
                        
                        available_drills = drill_categories.get(explore_category, [])
                        
                        if available_drills:
                            drill_category = st.selectbox(
                                "Drill down to:",
                                available_drills,
                                format_func=lambda x: {
                                    'payer': 'Payer',
                                    'organization': 'Organization',
                                    'taxonomy': 'Taxonomy',
                                    'procedure_set': 'Procedure Set',
                                    'procedure_class': 'Procedure Class'
                                }[x],
                                key="drill_category"
                            )
                            
                            if st.button("🔍 Drill Down", key="drill_down_button"):
                                with st.spinner("Drilling down..."):
                                    drill_data = fetch_data("/api/explore/drill-down", {
                                        "state": selected_state,
                                        "year_month": selected_year_month,
                                        "category": explore_category,
                                        "selected_value": drill_down_value,
                                        "drill_category": drill_category,
                                        "limit": 25
                                    })
                                
                                if drill_data and drill_data.get("results"):
                                    drill_results = drill_data["results"]
                                    
                                    st.success(f"Found {len(drill_results)} related {drill_category} values")
                                    
                                    # Display drill down results
                                    df_drill = pd.DataFrame(drill_results)
                                    
                                    # Format for display
                                    display_drill_df = df_drill.copy()
                                    display_drill_df['avg_rate'] = display_drill_df['avg_rate'].apply(lambda x: f"${x:.2f}")
                                    display_drill_df['min_rate'] = display_drill_df['min_rate'].apply(lambda x: f"${x:.2f}")
                                    display_drill_df['max_rate'] = display_drill_df['max_rate'].apply(lambda x: f"${x:.2f}")
                                    display_drill_df['record_count'] = display_drill_df['record_count'].apply(lambda x: f"{x:,}")
                                    display_drill_df['unique_providers'] = display_drill_df['unique_providers'].apply(lambda x: f"{x:,}")
                                    display_drill_df['unique_procedures'] = display_drill_df['unique_procedures'].apply(lambda x: f"{x:,}")
                                    
                                    # Rename columns
                                    display_drill_df = display_drill_df.rename(columns={
                                        'value': 'Value',
                                        'record_count': 'Records',
                                        'unique_providers': 'Providers',
                                        'unique_procedures': 'Procedures',
                                        'avg_rate': 'Avg Rate',
                                        'min_rate': 'Min Rate',
                                        'max_rate': 'Max Rate'
                                    })
                                    
                                    st.dataframe(
                                        display_drill_df[['Value', 'Records', 'Providers', 'Procedures', 'Avg Rate', 'Min Rate', 'Max Rate']],
                                        use_container_width=True,
                                        height=300
                                    )
                                else:
                                    st.info(f"No related {drill_category} data found for {drill_down_value}")
                        else:
                            st.info("No drill down options available for this category.")
                else:
                    st.info(f"No {explore_category} data found for the selected state and time period.")
            else:
                st.error("Could not load category statistics.")
        else:
            st.info("👆 Select a category above to start exploring your data!")
            
            # Show some helpful information
            st.markdown("""
            ### 🎯 How to Use Data Explorer
            
            1. **Select a Category**: Choose from payer, organization, taxonomy, procedure set, or procedure class
            2. **View Overview**: See high-level statistics about your data
            3. **Explore Details**: Browse the most common values in your selected category
            4. **Drill Down**: Select a specific value to see related data in other categories
            5. **Use in Search**: Use your findings to refine your main search filters
            
            ### 💡 Tips
            
            - Start with **Payer** to see which insurance companies have data
            - Use **Taxonomy** to explore provider specialties
            - Try **Procedure Set** to understand procedure categories
            - Drill down to see relationships between different data dimensions
            """)
    
    # Footer
    st.markdown("---")
    st.markdown("**MRF Data Lookup Dashboard** - Powered by FastAPI & Streamlit")
    
    # Auto-refresh option
    if st.sidebar.checkbox("Auto-refresh (30s)", value=False):
        time.sleep(30)
        st.rerun()

if __name__ == "__main__":
    main()
