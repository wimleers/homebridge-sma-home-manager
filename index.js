const inherits = require("util").inherits,
	ModbusRTU = require("modbus-serial"),
	dgram = require('dgram');

var client = new ModbusRTU();

var Service, Characteristic, Accessory;

module.exports = function(homebridge) {
	Service = homebridge.hap.Service;
	Characteristic = homebridge.hap.Characteristic;
	Accessory = homebridge.hap.Accessory;

	homebridge.registerAccessory("homebridge-sma-home-manager", "SMAHomeManager", SMAHomeManager);
};

function SMAHomeManager(log, config) {
	// General.
	this.log = log;
	this.name = config["name"] || "SMA Solar Inverter";
	this.debug = config["debug"] || false;

	// Inverter: SMA Sunny Boy.
	// Hardcoded address and hence zero config thanks to https://manuals.sma.de/SBSxx-10/en-US/1685190283.html.
	this.inverterAddress = '169.254.12.3';
	const refreshInterval = (config['refreshInterval'] * 1000) || 1000;

	// Energy manager: SMA Home Manager 2.0.
	this.homeManagerAddress = '239.12.255.254';
	// @see https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=sma-spw
	this.homeManagerPort = 9522;
	this.multicastMembershipIntervalId = false;
	this.movingAverage = 0;
	this.movingAverageSampleCount = 0;
	this.movingAverageSampleSize = 3 * 60; // 3 minutes worth of data
	this.sampleCount = 0;
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
			maxValue: maxAmperes,
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
		this._refresh();
	}.bind(this), refreshInterval);

	// Listen to SMA Home Manager Speedwire datagrams.
	this._startListener();
}

SMAHomeManager.prototype = {

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

	_refresh: function() {
		// Obtain the values
		try {
			/*
			// Serial Number
			client.readHoldingRegisters(30057, 10, function(err, data) {this.value.SerialNumber = data.buffer.readUInt32BE();}.bind(this));
			*/

			// Inverter: StatusActive & StatusFault characteristics
			client.readHoldingRegisters(30201, 10, function(err, data) {
				const condition = data.buffer.readUInt32BE();
				// 35 = Fault
				if (condition === 35) {
					this.inverter.getCharacteristic(Characteristic.StatusActive).updateValue(false);
					this.inverter.getCharacteristic(Characteristic.StatusFault).updateValue(Characteristic.StatusFault.GENERAL_FAULT);
				}
				// 455 = Warning
				else if (condition === 455) {
					this.inverter.getCharacteristic(Characteristic.StatusActive).updateValue(True);
					this.inverter.getCharacteristic(Characteristic.StatusFault).updateValue(Characteristic.StatusFault.GENERAL_FAULT);
				}
				// 303 = Off, 307 = Ok
				else {
					this.inverter.getCharacteristic(Characteristic.StatusFault).updateValue(Characteristic.StatusFault.NO_FAULT);
					if (condition !== 303 && condition !== 307) {
						this.log('Unknown inverter condition', condition);
					}
					this.inverter.getCharacteristic(Characteristic.StatusActive).updateValue(condition === 307);
				}
			}.bind(this));

			client.readHoldingRegisters(30775, 10, function(err, data) {
				// Check if the value is unrealistic (the inverter is not generating)
				if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
					const solarWatts = data.buffer.readUInt32BE();
					if(this.debug) {this.log('Current production:', solarWatts, 'Watt');}
					this.inverter.getCharacteristic(Characteristic.On).updateValue(solarWatts > 0);

					// Eve - Watts
					this.inverter.getCharacteristic(Characteristic.CustomWatts).updateValue(solarWatts);

					// Only when solar panels are currently producing can we set A & V.
					if (solarWatts > 0) {
						// Eve - Amperes
						client.readHoldingRegisters(30977, 10, function(err, data) {
							if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
								this.inverter.getCharacteristic(Characteristic.CustomAmperes).updateValue(data.buffer.readUInt32BE() / 1000);
							}
						}.bind(this));

						// Eve - Volts
						client.readHoldingRegisters(30783, 10, function(err, data) {
							if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*100) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
								this.inverter.getCharacteristic(Characteristic.CustomVolts).updateValue(data.buffer.readUInt32BE() / 100);
							}
						}.bind(this));
					}
				}
				else {
					this.inverter.getCharacteristic(Characteristic.On).updateValue(false);
					this.inverter.getCharacteristic(Characteristic.CustomWatts).updateValue(0);
				}
			}.bind(this));

			// Eve - kWh
			client.readHoldingRegisters(30535, 10, function(err, data) {
				if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
					this.inverter.getCharacteristic(Characteristic.CustomKilowattHours).updateValue(data.buffer.readUInt32BE() / 1000);
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
			const [timestamp, netWatts, version] = this._parseDatagram(msg, rinfo);

			// Net export/import: real-time.
			this.importRealtime.getCharacteristic(Characteristic.On).updateValue(netWatts > 0);
			this.exportRealtime.getCharacteristic(Characteristic.On).updateValue(netWatts <= 0);
			this.importRealtime.getCharacteristic(Characteristic.CustomWatts).updateValue(netWatts > 0 ? netWatts : 0);
			this.exportRealtime.getCharacteristic(Characteristic.CustomWatts).updateValue(netWatts <= 0 ? -netWatts : 0);

			// Net export/import: moving average.
			this.sampleCount++;
const previousMovingAverage = this.movingAverage;
			if (this.movingAverageSampleCount < this.movingAverageSampleSize) {
				this.movingAverageSampleCount++;
			}
			if (this.movingAverageSampleCount === 1) {
				this.movingAverage = netWatts;
			}
			else {
				this.movingAverage += (netWatts - this.movingAverage) / this.movingAverageSampleCount;
			}
			if (this.debug) {
				this.log.debug('5 min avg', this.sampleCount, timestamp, 'avg=', this.movingAverage, 'vs actual=', netWatts, ' -> delta: ', (this.movingAverage-netWatts), Math.round(new Date().valueOf() / 1000));
			}
			const avgNetWatts = this.movingAverage;
			this.import.getCharacteristic(Characteristic.On).updateValue(avgNetWatts > 0);
			this.export.getCharacteristic(Characteristic.On).updateValue(avgNetWatts <= 0);
			this.import.getCharacteristic(Characteristic.CustomWatts).updateValue(avgNetWatts > 0 ? avgNetWatts : 0);
			this.export.getCharacteristic(Characteristic.CustomWatts).updateValue(avgNetWatts <= 0 ? -avgNetWatts : 0);
		}.bind(this));

		// Actually start listening.
		this.socket.bind(this.homeManagerPort);
	},

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
		/*
		this.value.Model = msg.slice(18, 20).readUInt16BE();
		this.informationService.getCharacteristic(Characteristic.Model).updateValue(this.value.Model);
		this.value.SerialNumber = Buffer.from(msg.slice(20, 24)).readUInt32BE();
		this.log(this.value.SerialNumber);
		this.informationService.setCharacteristic(Characteristic.SerialNumber, this.value.SerialNumber);
		*/

		// 7. Measuring time (in ms, with overflow) ("Ticker Messzeitpunkt in ms (überlaufend)")
		const timestamp = msg.slice(24, 28).readUInt32BE() / 1000;

		var data = msg.slice(28, -4);
		var netWatts = null;
		var version = null;
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
			if (measurementType === 'version') {
				version = data.slice(0, 1).readUint8() + '.' + data.slice(1, 2).readUint8() + '.' + data.slice(2, 3).readUint8() + '.' + data.slice(3, 4).toString();
//				this.log(version);
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

		return [timestamp, netWatts, version];
	},

	getServices: function() {
		this.inverter = new Service.Outlet(this.name);
		// Inverter being on/off is something the inverter decides itself, so do not give the user the illusion they can change it.
		this._makeReadonly(this.inverter.getCharacteristic(Characteristic.On));
		this.inverter.addCharacteristic(Characteristic.StatusActive);
		this.inverter.addCharacteristic(Characteristic.StatusFault);
		this.inverter.addCharacteristic(Characteristic.CustomAmperes);
		this.inverter.addCharacteristic(Characteristic.CustomKilowattHours);
		this.inverter.addCharacteristic(Characteristic.CustomVolts);
		this.inverter.addCharacteristic(Characteristic.CustomWatts);
		this.inverter.setPrimaryService();

		this.import = new Service.Outlet("Import", "import");
		this._makeReadonly(this.import.getCharacteristic(Characteristic.On));
		this.import.addCharacteristic(Characteristic.CustomWatts);

		this.export = new Service.Outlet("Export", "export");
		this._makeReadonly(this.export.getCharacteristic(Characteristic.On));
		this.export.addCharacteristic(Characteristic.CustomWatts);

		this.importRealtime = new Service.Outlet("Import real-time", "import-realtime");
		this._makeReadonly(this.importRealtime.getCharacteristic(Characteristic.On));
		this.importRealtime.addCharacteristic(Characteristic.CustomWatts);

		this.exportRealtime = new Service.Outlet("Export real-time", "export-realtime");
		this._makeReadonly(this.exportRealtime.getCharacteristic(Characteristic.On));
		this.exportRealtime.addCharacteristic(Characteristic.CustomWatts);

		this.informationService = new Service.AccessoryInformation();
		this.informationService
			.setCharacteristic(Characteristic.Name, this.name)
			// @see https://github.com/homebridge/HAP-NodeJS/issues/940#issuecomment-1111470278
			.setCharacteristic(Characteristic.Manufacturer, 'SMA Solar Technology AG')
			.setCharacteristic(Characteristic.Model, 'Sunny Boy');

		return [
			this.inverter,
			this.import,
			this.export,
			this.importRealtime,
			this.exportRealtime,
			this.informationService
		];
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
