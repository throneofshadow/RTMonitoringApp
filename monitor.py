import pandas as pd
from datetime import datetime
import matplotlib.dates as mdates
import streamlit as st
import plotly.figure_factory as ff
import plotly.express as px
import matplotlib.pyplot as plt
import pdb
import numpy as np
import os
from scripts.database_query import DatabaseQuery
# Import testing file
DB = DatabaseQuery('app_test_data.csv')

time_plot = 200
client='Labrat-Testing'
branch_1_db = pd.DataFrame()
branch_2_db = pd.DataFrame()
for columns in DB.solar_production_records:
    if 'branch_1_' in columns:
        branch_1_db[columns] = DB.solar_production_records[columns]
    elif 'branch_2_' in columns:
        branch_2_db[columns] = DB.solar_production_records[columns]
    else:
        pass
# Plot branch 1 variables
# Get rid of all '0' variables
branch_1_db = branch_1_db.replace(0, np.nan).dropna(axis=1, how='all')
branch_2_db = branch_2_db.replace(0,np.nan).dropna(axis=1,how="all")


############

fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle('Rectifier Monitoring Dashboard for ' + client)
ax1.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_1_ac_current'][-1 * time_plot:-1] / 100, c='r', label='Blade 1 Current')
ax1.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_2_ac_current'][-1 * time_plot:-1]/ 100, c='g', label='Blade 2 Current')
ax1.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_3_ac_current'][-1 * time_plot:-1]/ 100, c='k', label='Blade 3 Current')
ax1.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_4_ac_current'][-1 * time_plot:-1]/ 100, c='b', label='Blade 4 Current')
ax1.set_ylabel('Blade Current (Amps)')
ax1.set_xlabel('Time (min)')
ax1.set_title('Rectifier Blade Input Currents')
ax1.legend()
ax1.xaxis.set_major_locator(plt.MaxNLocator(5))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax2.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_1_ac_voltage'][-1 * time_plot:-1]/100, c='r', label='Blade 1 Voltage')
ax2.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_2_ac_voltage'][-1 * time_plot:-1]/100, c='g', label='Blade 2 Voltage')
ax2.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_3_ac_voltage'][-1 * time_plot:-1]/100, c='k', label='Blade 3 Voltage')
ax2.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_4_ac_voltage'][-1 * time_plot:-1]/100, c='b', label='Blade 4 Voltage')
ax2.set_ylabel('Blade Voltage (Volts)')
ax2.set_xlabel('Time (min)')
ax2.set_title('Rectifier Blade Voltage')
ax2.legend()
ax2.xaxis.set_major_locator(plt.MaxNLocator(5))
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax3.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_1_enable'][-1 * time_plot:-1]+0.01, c='r', label='Blade 1 Status')
ax3.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_2_enable'][-1 * time_plot:-1]-0.01, c='g', label='Blade 2 Status')
ax3.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_3_enable'][-1 * time_plot:-1]+0.02, c='k', label='Blade 3 Status')
ax3.plot(DB.rectifier_control['timestamp'][-1* time_plot:-1], DB.rectifier_control['blade_4_enable'][-1 * time_plot:-1]-0.02, c='b', label='Blade 4 Status')
ax3.set_ylim(-0.1, 1.2)
ax3.set_ylabel('Blade Status')
ax3.set_xlabel('Time (min)')
ax3.set_title('Rectifier Status (Enable = 1)')
ax3.xaxis.set_major_locator(plt.MaxNLocator(5))
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax3.legend()
st.pyplot(fig=fig)

pdb.set_trace()
########

# Inverter plots
fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(14, 4))
ax1.plot(DB.inverter_record['timestamp'][-1* time_plot:-1], DB.inverter_record['ats_premises_connected'][-1 * time_plot:-1], label='ATS Connected')
ax1.set_ylabel('Premises Connected (1)')
ax1.set_xlabel('Time (min)')
ax1.set_title('Inverter records for PowerBloc: ATS Connected')
ax1.legend()
ax1.xaxis.set_major_locator(plt.MaxNLocator(5))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax2.plot(DB.inverter_record['timestamp'][-1* time_plot:-1], DB.inverter_record['hv_bus_voltage'][-1 * time_plot:-1] / 100.0, label='hv bus voltage')
ax2.set_ylabel('BUS Voltage (Volts)')
ax2.set_xlabel('Time (min)')
ax2.set_ylim(DB.inverter_record['hv_bus_voltage'][-1* time_plot:-1].min()/100 - 1,
             DB.inverter_record['hv_bus_voltage'][-1* time_plot:-1].max()/100 + 1)
ax2.set_title('Inverter High-Voltage BUS ')
ax2.legend()
ax2.xaxis.set_major_locator(plt.MaxNLocator(5))
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax2.set_ylim(DB.inverter_record['hv_bus_voltage'][-1* time_plot:-1].min()/100 - 1,
             DB.inverter_record['hv_bus_voltage'][-1* time_plot:-1].max()/100 + 1)
ax3.plot(DB.inverter_record['timestamp'][-1* time_plot:-1], DB.inverter_record['input_current'][-1 * time_plot:-1]/ 100.0, label='Input Current')
ax3.set_title('Inverter records for PowerBloc: Input Current')
ax3.set_ylabel('Current (Amps)')
ax3.set_xlabel('Time (min)')
ax3.set_ylim(DB.inverter_record['input_current'][-1* time_plot:-1].min()/100 - 1,
             DB.inverter_record['input_current'][-1* time_plot:-1].max()/100 + 1)
ax3.legend()
ax3.xaxis.set_major_locator(plt.MaxNLocator(5))
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
plt.show()

fig, ax = plt.subplots(1,1, figsize=(6, 3))
plt.plot(DB.control_status['timestamp'][-1 * time_plot::],
         DB.control_status['utility_on'][-1 * time_plot::]-0.02, c='b', linestyle='--', label='Utility On/Off')
plt.plot(DB.control_status['timestamp'][-1 * time_plot::],
         DB.control_status['solar_present'][-1 * time_plot::].astype(int), c='y',  label='Solar On/Off')  # Present only
plt.plot(DB.control_status['timestamp'][-1 * time_plot::],
         DB.control_status['generator_on'][-1 * time_plot::] + 0.03, c='k', label='Generator On/Off')
plt.plot(DB.control_status['timestamp'][-1 * time_plot:-1],
         DB.control_status['manual_estop_asserted'][-1 * time_plot:-1], c='r', label='E-STOP On/Off')
plt.plot(DB.control_status['timestamp'][-1 * time_plot:-1],
         DB.control_status['manual_estop_asserted'][-1 * time_plot:-1] + 0.015, c='r', linestyle='--', label='System Panic On/Off')
#plt.plot(DB.inverter_record['timestamp'][-1* time_plot:-1],
#         DB.inverter_record['ats_premises_connected'][-1 * time_plot:-1], linestyle='--', label='ATS Connected')
ax.xaxis.set_major_locator(plt.MaxNLocator(5))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
plt.legend()
plt.ylim(0, 1.2)
plt.ylabel('PowerBlock Control Logic')
plt.xlabel('Time (min)')
plt.title('PowerBlock Control Status')
plt.show()

###################
fig, (ax1, ax2, ax3) = plt.subplots(1,3,figsize=(18,5))
ax1.plot(DB.control_status['timestamp'][-1 * time_plot:-1],
         DB.control_status['unload_asserted'][-1 * time_plot:-1], c='r', label='Unload Phase Active')
ax1.plot(DB.control_status['timestamp'][-1 * time_plot:-1],
         DB.control_status['charging_allowed'][-1 * time_plot:-1], c='g', label='Charging Active')
#plt.plot(DB.control_status['timestamp'][-400:-1], DB.control_status['mid_peak'][-400:-1], c='y', label='Mid-Peak')
#ax1.title('Powerbloc Energy Peak Monitoring')
ax1.legend()
ax1.set_ylabel('True (1) / False (0)')
ax1.set_xlabel('Time (min)')
ax1.xaxis.set_major_locator(plt.MaxNLocator(5))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax2.plot(DB.control_status['timestamp'][-1 * time_plot::],
         DB.control_status['charge_state'][-1 * time_plot::].astype(int), label = 'Powercon Charging State')
ax2.legend()
ax2.set_ylabel('Charge State')
ax2.set_xlabel('Time (min)')
ax2.set_ylim(0,6)
ax2.xaxis.set_major_locator(plt.MaxNLocator(5))
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

ax3.plot(DB.control_status['timestamp'][-1 * time_plot:-1],
         DB.control_status['on_peak'][-1 * time_plot:-1], c='r', label='On-Peak')
ax3.plot(DB.control_status['timestamp'][-1 * time_plot:-1],
         DB.control_status['off_peak'][-1 * time_plot:-1], c='g', label='Off-Peak')
ax3.plot(DB.control_status['timestamp'][-1 * time_plot:-1],
         DB.control_status['mid_peak'][-1 * time_plot:-1], c='y', label='Mid-Peak')
ax3.legend()
ax3.set_ylabel('Activate Energy State (1)')
ax3.xaxis.set_major_locator(plt.MaxNLocator(5))
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
fig.suptitle('Powercon Energy State Monitoring')
plt.show()

###################

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
ax1.plot(DB.step_record['timestamp'][-1* time_plot:-1], DB.step_record['current'][-1*time_plot:-1]/ 10, label='STEP Current')
ax1.set_ylabel('Current (Amps)')
ax1.set_xlabel('Time (min)')
ax1.set_title('Total Solar Current')
ax1.legend()
ax1.xaxis.set_major_locator(plt.MaxNLocator(5))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax1.set_ylim(DB.step_record['current'][-1* time_plot:-1].min()/10 - 1, DB.step_record['current'][-1* time_plot:-1].max()/10 + 1)
#ax2.plot(DB.step_record['timestamp'][-1* time_plot:-1], DB.step_record['ground_fault_detected'][-1*time_plot:-1]+0.01, label='Ground Fault Detected')
#ax2.plot(DB.step_record['timestamp'][-1* time_plot:-1], DB.step_record['arc_fault_detected'][-1*time_plot:-1]-0.01, label='Arc Fault Detected')
#ax2.plot(DB.step_record['timestamp'][-1* time_plot:-1], DB.step_record['supervision_error'][-1*time_plot:-1], label='Supervision Error Detected')
#ax2.plot(DB.step_record['timestamp'][-1* time_plot:-1], DB.step_record['solar24_detected'][-1*time_plot:-1], label='Solar24 Detected')
ax2.set_xlabel('Time (min)')
ax2.set_ylabel('Signal Detected (1)')
ax2.set_title('STEP Control Signal Dashboard')
ax2.legend()
ax2.xaxis.set_major_locator(plt.MaxNLocator(5))
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
fig.suptitle('STEP Dashboard')
plt.show()


DB.twin_records['timestamp_p'] = DB.twin_records['timestamp'].dt.strftime('%H:%M')
#pdb.set_trace()
#ax.set_xticks(range(0,24), labels=range(0, 24))
#plt.title(client + '  Twin bus currents')
#plt.legend()
#plt.show()

