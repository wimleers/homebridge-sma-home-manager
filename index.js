/*
 * Copyright (c) 2022–2023 GPL Wim Leers.
 *
 *  This file is free software: you may copy, redistribute and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation, either version 2 of the License, or (at your
 *  option) any later version.
 *
 *  This file is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see .
 *
 * This file incorporates work covered by the following copyright and
 * permission notice:
 *
 *     Copyright (c) 2019 codyc1515
 *
 *     Permission is hereby granted, free of charge, to any person obtaining a copy
 *     of this software and associated documentation files (the "Software"), to deal
 *     in the Software without restriction, including without limitation the rights
 *     to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *     copies of the Software, and to permit persons to whom the Software is
 *     furnished to do so, subject to the following conditions:
 *
 *     The above copyright notice and this permission notice shall be included in all
 *     copies or substantial portions of the Software.
 *
 *     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 */
const inherits = require("util").inherits,
	ModbusRTU = require("modbus-serial"),
	dgram = require('dgram');

const PLATFORM = 'SMAHomeManager';
const PLUGIN_NAME = 'homebridge-sma-home-manager';

var client = new ModbusRTU();

var Service, Characteristic, Accessory;

module.exports = function(homebridge) {
	Service = homebridge.hap.Service;
	Characteristic = homebridge.hap.Characteristic;
	Accessory = homebridge.hap.Accessory;
	Uuid = homebridge.hap.uuid;

	homebridge.registerPlatform(PLUGIN_NAME, PLATFORM, SMAHomeManager);
};

function SMAHomeManager(log, config, api) {
	// General.
	this.log = log;
	this.name = config["name"] || "Solar Panels";
	this.debug = config["debug"] || false;

	// Platform state.
	// @see APIEvent.DID_FINISH_LAUNCHING
	// @see accessories()
	this.launched = false;
	// Discover both the inverter & energy manager prior to launching.
	// @see accessories()
	this.discovered = {};
	// The 3 accessories: live, recent, today.
	// @see accessories()
	this.live = null;
	this.recent = null;
	this.today = null;
	// How many minutes "recent" should track.
	this.recentMinutes = 3;
	// The measurements necessary for "recent".
	this.measurements = [];
	this.measurementsNeeded = this.recentMinutes * 60;
	this.nextMeasurementIndex = 0;

	// Inverter: SMA Sunny Boy.
	// Hardcoded address and hence zero config thanks to https://manuals.sma.de/SBSxx-10/en-US/1685190283.html.
	this.inverterAddress = '169.254.12.3';
	const refreshInterval = (config['refreshInterval'] * 1000) || 1000;

	// Energy manager: SMA Home Manager 2.0.
	this.homeManagerAddress = '239.12.255.254';
	// @see https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=sma-spw
	this.homeManagerPort = 9522;
	this.multicastMembershipIntervalId = false;
	// 230 volts is expected, safety threshold is 250, 40 amps.
	const maxVolts = 250;
	const maxAmperes = 40;
	const maxRealPowerTransmissionCapability = maxVolts * maxAmperes;

	Characteristic.CustomAmperes = function() {
		Characteristic.call(this, 'Amperes', 'E863F126-079E-48FF-8F27-9C2605A29F52');
		this.setProps({
			format: Characteristic.Formats.FLOAT,
			unit: 'A',
			minValue: 0,
			maxValue: maxAmperes,
			minStep: 0.01,
			perms: [Characteristic.Perms.READ, Characteristic.Perms.NOTIFY]
		});
		this.value = this.getDefaultValue();
	};
	inherits(Characteristic.CustomAmperes, Characteristic);
	Characteristic.CustomAmperes.UUID = 'E863F126-079E-48FF-8F27-9C2605A29F52';

	Characteristic.CustomKilowattHours = function() {
		Characteristic.call(this, 'Total Consumption', 'E863F10C-079E-48FF-8F27-9C2605A29F52');
		this.setProps({
			format: Characteristic.Formats.FLOAT,
			unit: 'kWh',
			minValue: 0,
			maxValue: 65535,
			minStep: 0.001,
			perms: [Characteristic.Perms.READ, Characteristic.Perms.NOTIFY]
		});
		this.value = this.getDefaultValue();
	};
	inherits(Characteristic.CustomKilowattHours, Characteristic);
	Characteristic.CustomKilowattHours.UUID = 'E863F10C-079E-48FF-8F27-9C2605A29F52';

	Characteristic.CustomVolts = function() {
		Characteristic.call(this, 'Volts', 'E863F10A-079E-48FF-8F27-9C2605A29F52');
		this.setProps({
			format: Characteristic.Formats.FLOAT,
			unit: 'V',
			minValue: 0,
			maxValue: maxVolts,
			minStep: 0.1,
			perms: [Characteristic.Perms.READ, Characteristic.Perms.NOTIFY]
		});
		this.value = this.getDefaultValue();
	};
	inherits(Characteristic.CustomVolts, Characteristic);
	Characteristic.CustomVolts.UUID = 'E863F10A-079E-48FF-8F27-9C2605A29F52';

	Characteristic.CustomWatts = function() {
		Characteristic.call(this, 'Consumption', 'E863F10D-079E-48FF-8F27-9C2605A29F52');
		this.setProps({
			format: Characteristic.Formats.FLOAT,
			unit: 'W',
			minValue: 0,
			maxValue: maxRealPowerTransmissionCapability,
			minStep: 0.1,
			perms: [Characteristic.Perms.READ, Characteristic.Perms.NOTIFY]
		});
		this.value = this.getDefaultValue();
	};
	inherits(Characteristic.CustomWatts, Characteristic);
	Characteristic.CustomWatts.UUID = 'E863F10D-079E-48FF-8F27-9C2605A29F52';

	// Connect to SMA Sunny boy inverter via ModBus.
	this._connect();
	setInterval(function() {
		this._readInverterMetadata();
		this._refresh();

		if (this.accessoriesCallback) {
			// Only launch after we got metadata from both inverter & energy manager.
			if (this.discovered.inverter && this.discovered.energyManager) {
				// Prevent race condition: store the callback locally and overwrite it.
				const callback = this.accessoriesCallback;
				this.accessoriesCallback = false;

				this.log.debug('Discovered', this.discovered);

				// Expose serial numbers & firmware revisions.
				this._setAccessoryInformation(this.live);
				this._setAccessoryInformation(this.recent);
				this._setAccessoryInformation(this.today);

				// Launch!
				callback([
					this.live,
					this.recent,
					this.today,
				])
				this.launched = true;
			}
			else {
				this.log.info('Discovered SMA inverter:', this.discovered.inverter ? this.discovered.inverter : 'no');
				this.log.info('Discovered SMA energy manager:', this.discovered.energyManager ? this.discovered.energyManager : 'no');
			}
		}
	}.bind(this), refreshInterval);

	// Listen to SMA Home Manager Speedwire datagrams.
	this._startListener();
}

SMAHomeManager.prototype = {

	_setAccessoryInformation(accessory) {
		const serialNumbers = this.discovered.inverter.SerialNumber + ' & ' + this.discovered.energyManager.SerialNumber;
		const firmwareRevisions = this.discovered.inverter.FirmwareRevision + ' & ' + this.discovered.energyManager.FirmwareRevision;
		accessory.getService(Service.AccessoryInformation)
			// @see https://github.com/homebridge/HAP-NodeJS/issues/940#issuecomment-1111470278
			.setCharacteristic(Characteristic.Manufacturer, 'SMA Solar Technology AG')
			.setCharacteristic(Characteristic.Model, 'Sunny Boy & SMA Home Manager 2.0')
			.setCharacteristic(Characteristic.SerialNumber, serialNumbers)
			.setCharacteristic(Characteristic.FirmwareRevision, firmwareRevisions);
	},

	// Adds 4 services to the given accessory: production, import, export, consumption.
	_addServicesToAccessory(accessory, suffix) {
		suffix = (suffix === undefined) ? '' : ' ' + suffix;

		const inverter = new Service.Outlet('Solar Panels' + suffix, "production");
		this._ensureAppropriateName(inverter);
		// Inverter being on/off is something the inverter decides itself, so do not give the user the illusion they can change it.
		this._makeReadonly(inverter.getCharacteristic(Characteristic.On));
		inverter.addCharacteristic(Characteristic.StatusActive);
		inverter.addCharacteristic(Characteristic.StatusFault);
		inverter.addCharacteristic(Characteristic.CustomAmperes);
		inverter.addCharacteristic(Characteristic.CustomKilowattHours);
		inverter.addCharacteristic(Characteristic.CustomVolts);
		inverter.addCharacteristic(Characteristic.CustomWatts);
		inverter.setPrimaryService();
		accessory.addService(inverter);

		const netImport = new Service.Outlet("Import" + suffix, "import");
		this._ensureAppropriateName(netImport);
		this._makeReadonly(netImport.getCharacteristic(Characteristic.On));
		netImport.addCharacteristic(Characteristic.CustomWatts);
		accessory.addService(netImport);

		const netExport = new Service.Outlet("Export" + suffix, "export");
		this._ensureAppropriateName(netExport);
		this._makeReadonly(netExport.getCharacteristic(Characteristic.On));
		netExport.addCharacteristic(Characteristic.CustomWatts);
		accessory.addService(netExport);

		const consumption = new Service.Outlet("Consumption" + suffix, "consumption");
		this._ensureAppropriateName(consumption);
		this._makeReadonly(consumption.getCharacteristic(Characteristic.On));
		consumption.addCharacteristic(Characteristic.CustomWatts);
		accessory.addService(consumption);

		// TRICKY: for static platforms, this is apparently not provided by Homebridge 🤷‍♂️
		accessory.getServices = function() {
			return accessory.services;
		}.bind(this);
	},

	accessories(callback) {
		this.live = new Accessory('Live', Uuid.generate(PLATFORM + 'live'))
		// TRICKY: work around homebridge/homebridge#2815
		this.live.name = this.live.displayName;
		this._addServicesToAccessory(this.live);

		this.recent = new Accessory('Recent', Uuid.generate(PLATFORM + 'recent'))
		// TRICKY: work around homebridge/homebridge#2815
		this.recent.name = this.recent.displayName;
		this._addServicesToAccessory(this.recent, 'Recent');

		this.today = new Accessory('Today', Uuid.generate(PLATFORM + 'today'))
		// TRICKY: work around homebridge/homebridge#2815
		this.today.name = this.today.displayName;
		this._addServicesToAccessory(this.today, 'Today');

		// Store the callback; we'll call it after discovery finishes.
		// @see this.discovered
		this.accessoriesCallback = callback;
	},

	identify: function(callback) {
		this.log("identify");
		callback();
	},

	_connect: function() {
		if(this.debug) {this.log("Attempting connection", this.inverterAddress);}

		// Connect to the ModBus server IP address
		try {
			client.connectTCP(this.inverterAddress);
		}
		catch(err) {
			this.log("Connection attempt failed");
			return;
		}

		try {
			// Set the ModBus Id to use
			client.setID(3);

			if(this.debug) {this.log("Connection successful");}
		}
		catch(err) {this.log("Could not set the Channel Number");}
	},

	_readInverterMetadata: function () {
			if (this.discovered.inverter) {
				return;
			}

			let serialNumber;
			let firmwareRevision;

			// Read serial number.
			client.readHoldingRegisters(30057, 10, function(err, data) {
				serialNumber = data.buffer.readUInt32BE();
				if (firmwareRevision) {
					this.discovered.inverter = {
						SerialNumber: serialNumber,
						FirmwareRevision: firmwareRevision,
					};
				}
			}.bind(this));

			//  Read firmware version.
			client.readHoldingRegisters(40063, 10, function(err, data) {
				// Per section 3.5.9, "SMA Firmware Data Formats":
				// - Byte 1: BCD-coded "major" version
				const major = data.buffer.slice(0, 1).readUint8();
				// - Byte 2: BCD-coded "minor" version
				const minor = data.buffer.slice(1, 2).readUint8();
				// - Byte 3: non-BCD-coded "build" version
				// @todo The third number should be 55, but the 0x35 being received is 53. Unsure how to parse.
				let build = data.buffer.slice(2, 3).readUint8();
				// Byte 4 contains teh release type with 0–5 mapped to a string, and >5 without special interpretation.
				let releaseType = data.buffer.slice(3, 4).readUint8();
				switch (releaseType) {
					case 0:
						releaseType = 'N';
						break;
					case 1:
						releaseType = 'E(xperimental)';
						break;
					case 2:
						releaseType = 'A(lpha)';
						break;
					case 3:
						releaseType = 'B(eta)';
						break;
					case 4:
						releaseType = 'R';
						break;
					case 5:
						releaseType = 'S(pecial release)';
						break;
				}
				const firmwareRevision = major + '.' + minor + '.' + build + '.' + releaseType;
				if (serialNumber) {
					this.discovered.inverter = {
						SerialNumber: serialNumber,
						FirmwareRevision: firmwareRevision,
					};
				}
			}.bind(this));
	},

	_refresh: function() {
		if (!this.launched) {
			return;
		}

		// Obtain the values
		try {
			const inverter = this.live.getServiceById(Service.Outlet, 'production');

			// Inverter: StatusActive & StatusFault characteristics
			client.readHoldingRegisters(30201, 10, function(err, data) {
				const condition = data.buffer.readUInt32BE();
				// 35 = Fault
				if (condition === 35) {
					inverter.getCharacteristic(Characteristic.StatusActive).updateValue(false);
					inverter.getCharacteristic(Characteristic.StatusFault).updateValue(Characteristic.StatusFault.GENERAL_FAULT);
				}
				// 455 = Warning
				else if (condition === 455) {
					inverter.getCharacteristic(Characteristic.StatusActive).updateValue(True);
					inverter.getCharacteristic(Characteristic.StatusFault).updateValue(Characteristic.StatusFault.GENERAL_FAULT);
				}
				// 303 = Off, 307 = Ok
				else {
					inverter.getCharacteristic(Characteristic.StatusFault).updateValue(Characteristic.StatusFault.NO_FAULT);
					if (condition !== 303 && condition !== 307) {
						this.log('Unknown inverter condition', condition);
					}
					inverter.getCharacteristic(Characteristic.StatusActive).updateValue(condition === 307);
				}
			}.bind(this));

			client.readHoldingRegisters(30775, 10, function(err, data) {
				// Check if the value is unrealistic (the inverter is not generating)
				if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
					const solarWatts = data.buffer.readUInt32BE();
					if(this.debug) {this.log('Current production:', solarWatts, 'Watt');}
					inverter.getCharacteristic(Characteristic.On).updateValue(solarWatts > 0);

					// Eve - Watts
					inverter.getCharacteristic(Characteristic.CustomWatts).updateValue(solarWatts);

					// Only when solar panels are currently producing can we set A & V.
					if (solarWatts > 0) {
						// Eve - Amperes
						client.readHoldingRegisters(30977, 10, function(err, data) {
							if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
								inverter.getCharacteristic(Characteristic.CustomAmperes).updateValue(data.buffer.readUInt32BE() / 1000);
							}
						}.bind(this));

						// Eve - Volts
						client.readHoldingRegisters(30783, 10, function(err, data) {
							if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*100) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
								inverter.getCharacteristic(Characteristic.CustomVolts).updateValue(data.buffer.readUInt32BE() / 100);
							}
						}.bind(this));
					}
				}
				else {
					inverter.getCharacteristic(Characteristic.On).updateValue(false);
					inverter.getCharacteristic(Characteristic.CustomWatts).updateValue(0);
				}
			}.bind(this));

			// Eve - kWh
			client.readHoldingRegisters(30535, 10, function(err, data) {
				if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
					inverter.getCharacteristic(Characteristic.CustomKilowattHours).updateValue(data.buffer.readUInt32BE() / 1000);
				}
			}.bind(this));
		}
		catch(err) {
			this.log("Refresh failed", "Attempting reconnect...", err);

			// Attempt to reconnect
			this._connect();
		}
	},

	_startListener: function() {
		this.socket = dgram.createSocket({type: 'udp4', reuseAddr: true});

		this.socket.on('error', function(err) {
			this.log.error('SMA Home Manager listening error!');
			this.importRealtime.getCharacteristic(Characteristic.On).updateValue(false);
			this.exportRealtime.getCharacteristic(Characteristic.On).updateValue(false);
			this.importService.getCharacteristic(Characteristic.On).updateValue(false);
			this.exportService.getCharacteristic(Characteristic.On).updateValue(false);
			this.log.error(err);
			this.clearInterval(this.multicastMembershipIntervalId);
			this._restartListener();
		}.bind(this));

		this.socket.on('listening', function() {
			this.socket.addMembership(this.homeManagerAddress);
			this.multicastMembershipIntervalId = setInterval(this._keepMembershipActive.bind(this), 120*1000);
		}.bind(this));

		this.socket.on('message', function(msg, rinfo) {
			if (!this._isValidDatagram(msg)) {
				return;
			}
			const [timestamp, netWatts] = this._parseDatagram(msg, rinfo);

			if (!this.launched) {
				return;
			}

			// Retrieve inverter production data stored by _refresh().
			const producedWatts = this.live.getServiceById(Service.Outlet, 'production').getCharacteristic(Characteristic.CustomWatts).value;
			this._processMeasurement(producedWatts, netWatts, timestamp);
		}.bind(this));

		// Actually start listening.
		this.socket.bind(this.homeManagerPort);
	},

	// Processes a measurement:
	// 1. Adds it to this.measurements
	// 2. Updates HomeKit.
	//
	// (Should be called in regular intervals, 1 s assumed.)
	_processMeasurement: function(producedWattsFromInverter, netWattsFromEnergyManager, timestampFromEnergyManager) {
		// Capture the observations in a "measurement" object.
		const importWatts = netWattsFromEnergyManager > 0 ? netWattsFromEnergyManager: 0;
		const exportWatts = netWattsFromEnergyManager < 0 ? -netWattsFromEnergyManager: 0;
		const measurement = {
			timestamp: timestampFromEnergyManager,
			import: importWatts,
			export: exportWatts,
			production: producedWattsFromInverter,
			consumption: importWatts + producedWattsFromInverter,
		};

		// Store measurement and compute next measurement index.
		var currentIndex = this.nextMeasurementIndex;
		this.measurements[currentIndex] = measurement;
		this.nextMeasurementIndex = (currentIndex + 1) % this.measurementsNeeded;

		// Provide a sequential view of the measurements, to faciliate recency metrics.
		// (The latest measurement is at the end.)
		const sequentialMeasurements = currentIndex === this.measurements.length - 1
			? this.measurements
			: this.measurements
				.slice(-this.measurements.length + currentIndex + 1)
				.concat(this.measurements.slice(0, currentIndex + 1));

		// Update each of the 4 services for both "live" and "recent".
		['import', 'export', 'production', 'consumption'].forEach(type => {
			// Live.
			const watts = this.measurements[currentIndex][type];
			const live = this.live.getServiceById(Service.Outlet, type);
			live.getCharacteristic(Characteristic.On).updateValue(watts > 0);
			live.getCharacteristic(Characteristic.CustomWatts).updateValue(watts);
			// Recent.
			const avgWatts = sequentialMeasurements.slice(-1 * this.recentMinutes * 60)
				.map(measurement => measurement[type])
				.reduce(this._reduceToAvg, 0);
			const recent = this.recent.getServiceById(Service.Outlet, type);
			recent.getCharacteristic(Characteristic.On).updateValue(avgWatts > 0);
			recent.getCharacteristic(Characteristic.CustomWatts).updateValue(avgWatts);
		})
	},

	_reduceToAvg: (avg, v, _, { length }) => avg + v / length,

	// TRICKY: https://github.com/nodejs/node/issues/39377
	// TRICKY: https://datatracker.ietf.org/doc/html/rfc3376#section-8.2
	_keepMembershipActive: function() {
		if (this.debug) {
			this.log.debug('Dropping and re-adding multicast membership');
		}
		this.socket.dropMembership(this.homeManagerAddress);
		this.socket.addMembership(this.homeManagerAddress);
	},

	_stopListener: function(cb) {
		this.socket.close(cb);
	},

	_restartListener: function() {
		this._stopListener(function() {
			setTimeout(this.startListener.bind(this), 10 * 1000)
		}.bind(this));
	},

	_getChannelTypeFromObisHeader: function(obis) {
		// SMA-specific identifier.
		const channel = obis.slice(0, 1).readUInt8();
		if (channel === 144) {
			return ['version', 0, 4];
		}

		// OBIS identifier. Channel is irrelevant.
		const measuredValueIndex = obis.slice(1, 2).readUInt8();
		const measurementType = obis.slice(2, 3).readUInt8();
		switch (measurementType) {
			case 4:
				return ['current', measuredValueIndex, 4];
			case 8:
				return ['meter', measuredValueIndex, 8];
		}
	},

	_isValidDatagram: function(datagram) {
		// See EMETER-Protocol-TI-en-10.pdf.
		const expectedHeader = Buffer.from('SMA\0');
		const expectedTag1 = Buffer.from([0x02, 0xa0]);
		const expectedTag2 = Buffer.from([0x00, 0x10]);
		const protocolIdForEnergyMeter = Buffer.from([0x60, 0x69]);
		const protocolIdForSpeedwireDiscovery = Buffer.from([0x60, 0x65]);

		// 1. Check expected header (bytes 0–3).
		var header = datagram.slice(0, 4);
		if (!header.equals(expectedHeader)) {
			this.log.error('Invalid datagram header found, discarding datagram.', header);
			return false;
		}
		// 2. Check expected tags (bytes 6–7 and 14–15).
		var tag1 = datagram.slice(6, 8);
		var tag2 = datagram.slice(14, 16);
		if (!tag1.equals(expectedTag1) || !tag2.equals(expectedTag2)) {
			this.log.error('Valid datagram found, but with unknown structure. Discarding.', tag1, tag2);
			return false;
		}
		// 3. Check expected protocol ID (bytes 16–17).
		var protocolId = datagram.slice(16, 18);
		if (!protocolId.equals(protocolIdForEnergyMeter)) {
			if (protocolId.equals(protocolIdForSpeedwireDiscovery)) {
				// this.log.debug('Valid datagram found, but for Speedwire discovery. Ignoring.');
			}
			else {
				this.log.error('Valid datagram found, but with unknown structure. Discarding.', protocolId);
			}
			return false;
		}
		// 4. Variable but irrelevant:
		// - group (bytes 8–12)
		var group = datagram.slice(8, 12).readUInt32BE();
		// 5. Check that the data length is indeed 4 bytes.
		const bodyLengthIndicator = datagram.slice(4, 6).readUInt16BE();
		if (bodyLengthIndicator !== 4) {
			this.log.error('Valid datagram found, but with unexpected body length indicator. Discarding.', bodyLengthIndicator);
			return false;
		}
		// 5. Read the body length. Note that bytes 12–16 consitute the body lenght
		//.   indicator block, but that this is
		//.   A) currently hardcoeded to 4 bytes
		//.   B) that bytes 14-16 are already checked above
		// @todo move that logic here for clarity?
		var bodyLength = datagram.slice(12, 14).readUInt16BE();
		// 6. Check that the datagram ends correctly.
		const expectedTrailer = Buffer.from([0x00, 0x00, 0x00, 0x00]);
		const trailer = datagram.slice(-4);
		if (!trailer.equals(expectedTrailer)) {
			this.log.error('Invalid datagram trailer found. Discarding.', trailer);
			return false;
		}

		return true;
	},

	_parseDatagram: function(msg, info) {
		// 6. Energy meter identifier ("Zählerkennung")
		const homeManagerDeviceMetadata = {
			Model: msg.slice(18, 20).readUInt16BE(),
			SerialNumber: Buffer.from(msg.slice(20, 24)).readUInt32BE()
		};

		// 7. Measuring time (in ms, with overflow) ("Ticker Messzeitpunkt in ms (überlaufend)")
		const timestamp = msg.slice(24, 28).readUInt32BE() / 1000;

		var data = msg.slice(28, -4);
		var netWatts = null;
		do {
			// Read OBIS header.
			const [measurementType, measuredValueIndex, byteCount] = this._getChannelTypeFromObisHeader(data.slice(0, 4));
			data = data.slice(4);

			// Read data based on OBIS identifier.
			if (measurementType === 'current') {
				if (measuredValueIndex === 1 || measuredValueIndex === 2) {
					const watts = data.slice(0, 4).readUInt32BE() / 10;
					if (watts > 0) {
						netWatts = measuredValueIndex === 1
							// Net import.
							? watts
							// Net export.
							: -1 * watts;
					}
				}
			}
			if (measurementType === 'version' && !this.launched && !this.discovered.energyManager) {
				const version = data.slice(0, 1).readUint8() + '.' + data.slice(1, 2).readUint8() + '.' + data.slice(2, 3).readUint8() + '.' + data.slice(3, 4).toString();
				this.discovered.energyManager = {
					...homeManagerDeviceMetadata,
					FirmwareRevision: version,
				};
			}
			// Proceed to next measuring point.
			data = data.slice(byteCount);
		} while (data.length > 0);

		/*
		// Debugging.
		for (const pair of msg.entries()) {
			this.log(pair);
		}
		*/

		return [timestamp, netWatts];
	},

	// Since iOS 16, `Name` must match `ConfiguredName`, otherwise iOS will automatically configure `ConfiguredName` based on the accessory name.
	// @see https://github.com/homebridge/homebridge/issues/3281#issuecomment-1338868527
	_ensureAppropriateName(service) {
		service.addCharacteristic(Characteristic.ConfiguredName);
		service.setCharacteristic(Characteristic.ConfiguredName, service.displayName);
		this._makeReadonly(service.getCharacteristic(Characteristic.ConfiguredName));
	},

	_makeReadonly(characteristic) {
		const readonlyPerms = [
			"pr" /* PAIRED_READ */,
			"ev" /* NOTIFY */,
		];
		characteristic.setProps({
			perms: characteristic.props.perms.filter(function (p) { return readonlyPerms.includes(p); })
		});
	}

};
