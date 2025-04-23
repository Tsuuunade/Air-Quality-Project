import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import duckdb
import pandas as pd

# initialize the Dash app
app = dash.Dash(__name__)

# define the overall layout of the app: 
# two tabs with different controls and graphs
app.layout = html.Div([
    dcc.Tabs([
        # first tab: a map of sensor locations
        dcc.Tab(
            label="Sensor Locations",
            children=[dcc.Graph(id="map-view")]
        ),
        # second tab: controls and plots for time series & distribution
        dcc.Tab(
            label="Parameter Plots",
            children=[
                # dropdown to select location
                dcc.Dropdown(
                    id="location-dropdown",
                    clearable=False,
                    multi=False,
                    searchable=True
                ),
                # dropdown to select which pollutant/parameter
                dcc.Dropdown(
                    id="parameter-dropdown",
                    clearable=False, 
                    multi=False,
                    searchable=True
                ),
                # date range picker for start/end dates
                dcc.DatePickerRange(
                    id="date-picker-range",
                    display_format="YYYY-MM-DD"
                    ),
                    dcc.Graph(id="line-plot"),
                    dcc.Graph(id="box-plot")
            ]
        )
    ])
])

@app.callback(
    Output("map-view", "figure"),
    Input("map-view", "id")
)
def update_map(_):
    """
    Callback to render the map of the latest air quality values per location.
    Triggered once on load (input is the map component's ID).
    """

    # connect to DuckDB in read-only mode
    with duckdb.connect("../air_quality.db", read_only=True) as db_connection:
        latest_values_df = db_connection.execute(
            "SELECT * FROM presentation.latest_param_values_per_location"
        ).fetchdf()

    # replace any missing values with zero for plotting
    latest_values_df.fillna(0, inplace=True)

    # create a Mapbox scatter plot of sensor locations
    map_fig = px.scatter_mapbox(
        latest_values_df,
        lat="lat",
        lon="lon",
        hover_name="location",
        hover_data={
            "lat": False,
            "lon": False,
            "datetime":True,
            "pm10": True,
            "pm25": True,
            "so2": True
        },
        zoom=6.0
    )

    # apply layout settings
    map_fig.update_layout(
        mapbox_style="open-street-map",
        height=800,
        title="Air Quality Monitoring Locations"
    )

    return map_fig


@app.callback(
    [
        Output("location-dropdown", "options"),
        Output("location-dropdown", "value"),
        Output("parameter-dropdown", "options"),
        Output("parameter-dropdown", "value"),
        Output("date-picker-range", "start_date"),
        Output("date-picker-range", "end_date"),
    ],
    Input("location-dropdown", "id"),
)
def update_dropdowns(_):
    """
    Populate the dropdowns and date picker with choices and defaults.
    Triggered on app load (input is dropdown ID, not its value).
    """

    # query the daily stats table for all entries
    with duckdb.connect("../air_quality.db", read_only=True) as db_connection:
        df = db_connection.execute(
            "SELECT * FROM presentation.daily_air_quality_stats"
        ).fetchdf()

     # build list of dicts for the location and parameter dropdown
    location_options = [
        {"label": location, "value": location} 
        for location in df["location"].unique()
    ]
    parameter_options = [
        {"label": parameter, "value": parameter}
        for parameter in df["parameter"].unique()
    ]

    # determine the available date range from the data
    start_date = df["measurement_date"].min()
    end_date = df["measurement_date"].max()

    return (
        location_options,
        df["location"].unique()[0],
        parameter_options,
        df["parameter"].unique()[0],
        start_date,
        end_date,
    )


@app.callback(
    [Output("line-plot", "figure"), Output("box-plot", "figure")],
    [
        Input("location-dropdown", "value"),
        Input("parameter-dropdown", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date")
    ]
)
def update_plots(selected_location, selected_parameter, start_date, end_date):
    """
    Generate the time-series line plot and weekday box plot
    based on user-selected location, parameter, and date range.
    """

    # read the full daily stats table
    with duckdb.connect("../air_quality.db", read_only=True) as db_connection:
        daily_stats_df = db_connection.execute(
            "SELECT * FROM presentation.daily_air_quality_stats"
        ).fetchdf()

    # filter data for the chosen location, parameter, and date range
    filtered_df = daily_stats_df[daily_stats_df["location"] ==
                                selected_location]
    filtered_df = filtered_df[filtered_df["parameter"] == selected_parameter]
    filtered_df = filtered_df[
        (filtered_df["measurement_date"] >= pd.to_datetime(start_date))
        & (filtered_df["measurement_date"] <= pd.to_datetime(end_date))
    ]

    # if no data remains, return empty placeholder figures
    if filtered_df.empty:
        empty_line = px.line(title="No data available for that selection")
        empty_box  = px.box(title="No data available for that selection")
        return empty_line, empty_box

    # extract the unit label for axis labeling
    unit_label = filtered_df["units"].iat[0]  # first element

    labels = {
        "average_value": unit_label,
        "measurement_date": "Date"
        }

    # time-series line plot, sorted by date
    line_fig = px.line(
        filtered_df.sort_values(by="measurement_date"),
        x="measurement_date",
        y="average_value",
        labels=labels,
        title=f"Plot Over Time of {selected_parameter} Levels"
    )

    # box plot of values by weekday, sorted to Monday→Sunday order
    box_fig = px.box(
        filtered_df.sort_values(by="weekday_number"),
        x="weekday",
        y="average_value",
        labels=labels,
        title=f"Distribution of {selected_parameter} Levels by Weekday"
    )

    return line_fig, box_fig


if __name__ == "__main__":
    app.run(debug=True)