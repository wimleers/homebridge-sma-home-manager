const inherits = require("util").inherits,
	moment = require('moment'),
	ModbusRTU = require("modbus-serial"),
	dns = require("dns");

var client = new ModbusRTU();

var Service, Characteristic, Accessory;

module.exports = function(homebridge) {
	Service = homebridge.hap.Service;
	Characteristic = homebridge.hap.Characteristic;
	Accessory = homebridge.hap.Accessory;

	homebridge.registerAccessory("homebridge-sma-home-manager", "SMAHomeManager", SMAHomeManager);
};

function SMAHomeManager(log, config) {
	this.log = log;
	this.name = config["name"] || "SMA Solar Inverter";
	this.hostname = config["hostname"];
	this.refreshInterval = (config['refreshInterval'] * 1000) || 60000;
	this.debug = config["debug"] || false;
	this.inverterToday = config["inverterToday"] || false;
	this.inverterTotal = config["inverterTotal"] || false;

	this.value = [];
	this.value.Name = config["name"] || '';
	this.value.Manufacturer = "";
	this.value.Model = "";
	this.value.FirmwareRevision = "1.0.0";
	this.value.SerialNumber = "";

	Characteristic.CustomAmperes = function() {
		Characteristic.call(this, 'Amperes', 'E863F126-079E-48FF-8F27-9C2605A29F52');
		this.setProps({
			format: Characteristic.Formats.FLOAT,
			unit: 'A',
			minValue: 0,
			maxValue: 65535,
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
			maxValue: 65535,
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
			maxValue: 65535,
			minStep: 0.1,
			perms: [Characteristic.Perms.READ, Characteristic.Perms.NOTIFY]
		});
		this.value = this.getDefaultValue();
	};
	inherits(Characteristic.CustomWatts, Characteristic);
	Characteristic.CustomWatts.UUID = 'E863F10D-079E-48FF-8F27-9C2605A29F52';

	// Start the connection and refresh cycles
	this._connect();

	// Set the automatic refresh onload only
	setInterval(function() {
		this._refresh();
	}.bind(this), this.refreshInterval);
}

SMAHomeManager.prototype = {

	identify: function(callback) {
		this.log("identify");
		callback();
	},

	_connect: function(isInitial) {
		if(this.debug) {this.log("Attempting connection", this.hostname);}

		// Get the hostname from dns - note: IPv4 support only for currently, have not tested IPv6
		try {
			dns.resolve(this.hostname, "A", function (err, addresses, family) {
				// Connect to the ModBus server hostname
				if(!err) {client.connectTCP(addresses[0]);}
				else {client.connectTCP(this.hostname);}
			}.bind(this));
		}
		catch(err) {
			this.log("Failed to resolve DNS hostname - maybe this is an IP address?", err);

			// Connect to the ModBus server IP address
			try {client.connectTCP(this.hostname);}
			catch(err) {this.log("Connection attempt failed");}
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
			// Manufacturer
			client.readHoldingRegisters(30051, 10, function(err, data) {
				switch(data.buffer.readUInt32BE()) {
					case "8001": this.value.Manufacturer = "SMA Solar Inverter"; break;
					default: this.value.Manufacturer = "Unknown"; break;
				}
			}.bind(this));

			// Model
			client.readHoldingRegisters(30053, 10, function(err, data) {
				switch(data.buffer.readUInt32BE()) {
					case "9319" : this.value.Model = "Sunny Boy 3.0"; break;
					case "9320" : this.value.Model = "Sunny Boy 3.6"; break;
					case "9321" : this.value.Model = "Sunny Boy 4.0"; break;
					case "9322" : this.value.Model = "Sunny Boy 5.0"; break;
					default: this.value.Model = "Sunny Boy"; break;
				}
			}.bind(this));

			// Serial Number
			client.readHoldingRegisters(30057, 10, function(err, data) {this.value.SerialNumber = data.buffer.readUInt32BE();}.bind(this));
			*/

			// Currently - Light Sensor
			client.readHoldingRegisters(30775, 10, function(err, data) {
				// Check if the value is unrealistic (the inverter is not generating)
				if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
					this.lightSensorCurrently.getCharacteristic(Characteristic.CurrentAmbientLightLevel).updateValue(data.buffer.readUInt32BE() / 1000);

					// Eve - Watts
					this.lightSensorCurrently.getCharacteristic(Characteristic.CustomWatts).updateValue(data.buffer.readUInt32BE());

					if(data.buffer.readUInt32BE() > 0) {
						if(this.debug) {this.log("Device status", "On");}
						
						// Today - Light Sensor
						if (this.inverterToday) {
							client.readHoldingRegisters(30535, 10, function(err, data) {
								if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
									this.lightSensorToday.getCharacteristic(Characteristic.CurrentAmbientLightLevel).updateValue(data.buffer.readUInt32BE() / 1000);
								}
							}.bind(this));
						}

						client.readHoldingRegisters(30529, 10, function(err, data) {
							if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
								// Eve - kWh
								this.lightSensorCurrently.getCharacteristic(Characteristic.CustomKilowattHours).updateValue(data.buffer.readUInt32BE() / 1000);
								// All Time - Light Sensor
								if (this.inverterTotal) {
									this.lightSensorTotal.getCharacteristic(Characteristic.CurrentAmbientLightLevel).updateValue(data.buffer.readUInt32BE() / 1000);
								}
							}
						}.bind(this));

						// Amperes - FakeGato
						client.readHoldingRegisters(30977, 10, function(err, data) {
							if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*1000) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
								this.lightSensorCurrently.getCharacteristic(Characteristic.CustomAmperes).updateValue(data.buffer.readUInt32BE() / 1000);
							}
						}.bind(this));

						// Volts - FakeGato
						client.readHoldingRegisters(30783, 10, function(err, data) {
							if(data.buffer.readUInt32BE() > 0 && data.buffer.readUInt32BE() <= (65535*100) && typeof data.buffer.readUInt32BE() == 'number' && Number.isFinite(data.buffer.readUInt32BE())) {
								this.lightSensorCurrently.getCharacteristic(Characteristic.CustomVolts).updateValue(data.buffer.readUInt32BE() / 100);
							}
						}.bind(this));
					}
					else if (this.debug) {this.log("Device status", "Off - low value");}
				}
				else {
					if(this.debug) {this.log("Device status", "Off - unreasonable value");}
					this.lightSensorCurrently.getCharacteristic(Characteristic.CurrentAmbientLightLevel).updateValue(0.0001);
				}
			}.bind(this));
		}
		catch(err) {
			this.log("Refresh failed", "Attempting reconnect...", err);

			// Attempt to reconnect
			this._connect();
		}
	},

	getServices: function() {
		this.lightSensorCurrently = new Service.LightSensor(this.name + " Currently", "currently");
		this.lightSensorCurrently.getCharacteristic(Characteristic.CurrentAmbientLightLevel)
			.setProps({
				unit: "kWh",
				minValue: 0,
				maxValue: 100000,
				minStep: 0.0001
			});
		this.lightSensorCurrently.addCharacteristic(Characteristic.CustomAmperes);
		this.lightSensorCurrently.addCharacteristic(Characteristic.CustomKilowattHours);
		this.lightSensorCurrently.addCharacteristic(Characteristic.CustomVolts);
		this.lightSensorCurrently.addCharacteristic(Characteristic.CustomWatts);

		if (this.inverterToday) {
			this.lightSensorToday = new Service.LightSensor(this.name + " Today", "today");
			this.lightSensorToday.getCharacteristic(Characteristic.CurrentAmbientLightLevel)
				.setProps({
					unit: "kWh",
					minValue: 0,
					maxValue: 100000,
					minStep: 0.0001
				});
		}

		if (this.inverterTotal)	 {
			this.lightSensorTotal = new Service.LightSensor(this.name + " Total", "total");
			this.lightSensorTotal.getCharacteristic(Characteristic.CurrentAmbientLightLevel)
				.setProps({
					unit: "kWh",
					minValue: 0,
					maxValue: 100000,
					minStep: 0.0001
				});
		}

		this.informationService = new Service.AccessoryInformation();
		this.informationService
			.setCharacteristic(Characteristic.Name, this.value.Name)
			.setCharacteristic(Characteristic.Manufacturer, this.value.Manufacturer)
			.setCharacteristic(Characteristic.Model, this.value.Model)
			.setCharacteristic(Characteristic.SerialNumber, this.value.SerialNumber);

		return [
			this.lightSensorCurrently,
			this.lightSensorToday,
			this.lightSensorTotal,
			this.informationService
		];
	},

	_getValue: function(CharacteristicName, callback) {
		if(this.debug) {this.log("GET", CharacteristicName);}
		callback(null);
	},

	_setValue: function(CharacteristicName, value, callback) {
		// This does nothing if the user tries to turn it on / off as we cannot action anything on the device
		if(this.debug) {this.log("SET", CharacteristicName);}
		callback(null, true);
	}

};
