const inherits = require("util").inherits,
	ModbusRTU = require("modbus-serial"),
	dgram = require('dgram');

const PLATFORM = 'SMAHomeManager';
const PACKAGE = require('./package.json');
const PLUGIN_NAME = PACKAGE.name;

// See SMA-Modbus-general-TI-en-10.pdf.
const SMA_MODBUS_CLIENT_ID = 3;
// In that same PDF, see "3.5.7 SMA Data Types and NaN Values".
const SMA_MODBUS_S32_NAN_VALUE = Buffer.from([0x80, 0x00, 0x00, 0x00]);
const SMA_MODBUS_U32_NAN_VALUE = Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]);

var client = new ModbusRTU();

var Service, Characteristic, Accessory;

module.exports = function(homebridge) {
	Service = homebridge.hap.Service;
	Characteristic = homebridge.hap.Characteristic;
	Accessory = homebridge.hap.Accessory;
	Uuid = homebridge.hap.uuid;
	HomebridgeAPI = homebridge;

	homebridge.registerPlatform(PLUGIN_NAME, PLATFORM, SMAHomeManager);
};

function SMAHomeManager(log, config, api) {
	this.log = log;

	// Platform state.
	// @see APIEvent.DID_FINISH_LAUNCHING
	// @see accessories()
	this.launched = false;
	// Discover both the inverter & energy manager prior to launching.
	// @see accessories()
	this.discovered = {};
	// The 3+ accessories: live, recent, today, signals (>=0).
	// @see accessories()
	this.live = null;
	this.recent = null;
	this.today = null;
	this.signals = {};
	// How many minutes "recent" should track.
	this.recentMinutes = 3;
	// The measurements necessary for "recent".
	this.measurements = [];
	this.measurementsNeeded = Math.max(this.recentMinutes, 15) * 60;
	this.currentMeasurementIndex = null;
	this.nextMeasurementIndex = 0;
	// Some state needs to be persisted because it cannot be queried from the
	// inverter nor the energy manager.
	this.storage = require('node-persist');
	this.storage.initSync({
		dir: HomebridgeAPI.user.persistPath(),
		forgiveParseErrors: true
	});

	// Inverter: SMA Sunny Boy.
	// Hardcoded address and hence zero config thanks to https://manuals.sma.de/SBSxx-10/en-US/1685190283.html.
	this.inverterAddress = '169.254.12.3';
	// TRICKY: SMA decided to not populate ModBus registers 30577 & 30579.
	// Consequently, today's "net export" and "net import" need to be computed
	// from total net export ("feed in counter", 30583) & total net import
	// ("grid counter", 30581)… also persist this across restarts.
	const cachedToday = this.storage.getItemSync(PLUGIN_NAME + 'computedToday');
	this.computedToday = cachedToday !== undefined
		? cachedToday
		: { day: -1, missedSeconds: -1, start: { totalExport: -1, totalImport: -1 }, now: { totalExport: -1, totalImport: -1 } };
	this.storage.setItemSync(PLUGIN_NAME + 'computedToday', this.computedToday);

	// Energy manager: SMA Home Manager 2.0.
	this.homeManagerAddress = '239.12.255.254';
	// @see https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=sma-spw
	this.homeManagerPort = 9522;
	this.multicastMembershipIntervalId = false;
	// 230 volts is expected, safety threshold is 250, 40 amps.
	const maxVolts = 250;
	const maxAmperes = 40;
	const maxRealPowerTransmissionCapability = maxVolts * maxAmperes;

	// Functionality on top of both the inverter & energy manager.
	this.signalsConfig = {
		builtIn: config.signals,
		surplus: config.surplusSignals || [],
	};
	// For small variations, like lights etc, which should be excluded from the PV surplus.
	this.baseLoadVariability = 50;

	// Define non-standard characteristics.
	const commonProps = {
		format: Characteristic.Formats.FLOAT,
		perms: [Characteristic.Perms.READ, Characteristic.Perms.NOTIFY],
	};
	const realPowerProps = {
		...commonProps,
		unit: 'W',
		minValue: 0,
		maxValue: maxRealPowerTransmissionCapability,
		minStep: 0.1,
	};
	const energyProps = {
		...commonProps,
		unit: 'kWh',
		minValue: 0,
		maxValue: 65535,
		minStep: 0.001,
	};
	const nonStandardCharacteristics = {
		// Eve characteristics.
		CustomWatts: { uuid: 'E863F10D-079E-48FF-8F27-9C2605A29F52', name: 'Consumption', props: realPowerProps },
		CustomKilowattHours: { uuid: 'E863F10C-079E-48FF-8F27-9C2605A29F52', name: 'Total Consumption', props: energyProps },
		CustomAmperes: { uuid: 'E863F126-079E-48FF-8F27-9C2605A29F52', name: 'Amperes', props: {
			...commonProps,
			unit: 'A',
			minValue: 0,
			maxValue: maxAmperes,
			minStep: 0.01,
		}},
		CustomVolts: { uuid: 'E863F10A-079E-48FF-8F27-9C2605A29F52', name: 'Volts', props: {
			...commonProps,
			unit: 'V',
			minValue: 0,
			maxValue: maxVolts,
			minStep: 0.1,
		}},
		// Our characteristics.
		CustomProduction: { uuid: '00000001-0000-1000-8000-000019880120', name: 'Production', props: realPowerProps },
		CustomImport: { uuid: '00000002-0000-1000-8000-000019880120', name: 'Import', props: { ...realPowerProps,  unit: 'W-' } },
		CustomExport: { uuid: '00000003-0000-1000-8000-000019880120', name: 'Export', props: { ...realPowerProps, unit: 'W+' } },
		CustomKilowattHoursProduction: { uuid: '00000011-0000-1000-8000-000019880120', name: 'Total Production', props: energyProps },
		CustomKilowattHoursImport: { uuid: '00000012-0000-1000-8000-000019880120', name: 'Total Import', props: { ...energyProps, unit: 'kWh-' } },
		CustomKilowattHoursExport: { uuid: '00000013-0000-1000-8000-000019880120', name: 'Total Export', props: { ...energyProps, unit: 'kWh+' } },
		CustomSelfSufficiency: { uuid: '00000021-0000-1000-8000-000019880120', name: 'Self-Sufficiency', props: {
			...commonProps,
			unit: Characteristic.Units.PERCENTAGE,
			minValue: -100,
			maxValue: 1000,
			minStep: 1,
		}},
		CustomReason: { uuid: '00001000-0000-1000-8000-000019880120', name: 'Reason', props: {
			...commonProps,
			format: Characteristic.Formats.STRING,
			maxLen: 256,
		}},
	};
	Object.keys(nonStandardCharacteristics).forEach(characteristic => {
		const definition = nonStandardCharacteristics[characteristic];
		Characteristic[characteristic] = function() {
			Characteristic.call(this, definition.name, definition.uuid);
			this.setProps(definition.props);
			this.value = this.getDefaultValue();
		};
		inherits(Characteristic[characteristic], Characteristic);
		Characteristic[characteristic].UUID = definition.uuid;
	});

	// Define non-standard services.
	Service.CustomPowerMonitor = function(displayName, subtype) {
		displayName = (displayName === undefined) ? 'Power Monitor' : displayName;
		Service.call(this, displayName, '10000000-0000-1000-8000-000019880120', subtype);
		// Required characteristics.
		this.addCharacteristic(Characteristic.CustomSelfSufficiency);
		this.addCharacteristic(Characteristic.CustomWatts);
		this.addCharacteristic(Characteristic.CustomProduction);
		this.addCharacteristic(Characteristic.CustomImport);
		this.addCharacteristic(Characteristic.CustomExport);
	};
	inherits(Service.CustomPowerMonitor, Service);
	Service.CustomPowerMonitor.UUID = '10000000-0000-1000-8000-000019880120';
	Service.CustomEnergyMonitor = function(displayName, subtype) {
		displayName = (displayName === undefined) ? 'Energy Monitor' : displayName;
		Service.call(this, displayName, '20000000-0000-1000-8000-000019880120', subtype);
		// Required characteristics.
		this.addCharacteristic(Characteristic.CustomSelfSufficiency);
		this.addCharacteristic(Characteristic.CustomKilowattHours);
		this.addCharacteristic(Characteristic.CustomKilowattHoursProduction);
		this.addCharacteristic(Characteristic.CustomKilowattHoursImport);
		this.addCharacteristic(Characteristic.CustomKilowattHoursExport);
	};
	inherits(Service.CustomEnergyMonitor, Service);
	Service.CustomEnergyMonitor.UUID = '20000000-0000-1000-8000-000019880120';
	Service.CustomEnergySignal = function(displayName, subtype) {
		displayName = (displayName === undefined) ? 'Energy Signal' : displayName;
		Service.call(this, displayName, '30000000-0000-1000-8000-000019880120', subtype);
		// Required characteristics.
		this.addCharacteristic(Characteristic.On);
		this.getCharacteristic(Characteristic.On).setProps({perms: [Characteristic.Perms.READ, Characteristic.Perms.NOTIFY]});
		this.addCharacteristic(Characteristic.CustomReason);
	};
	inherits(Service.CustomEnergySignal, Service);
	Service.CustomEnergySignal.UUID = '30000000-0000-1000-8000-000019880120';

	// Connect to SMA inverter via ModBus. Sporadic updates suffice because they affect only general status, V, A and daily totals;. SMA Home Manager provides the live data.
	this._connectToInverter();
	setInterval(function() {
		this._readInverterData();
	}.bind(this), 60 * 1000);

	// Listen to SMA Home Manager Speedwire datagrams.
	this._listenToHomeManager();

	// Launch after both inverter & energy manager are discovered.
	setInterval(function() {
		this._readInverterMetadata();
		if (this.accessoriesCallback) {
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
					...Object.values(this.signals),
				])
				this.launched = true;
			}
			else {
				this.log.info('Discovered SMA inverter:', this.discovered.inverter ? this.discovered.inverter : 'no');
				this.log.info('Discovered SMA energy manager:', this.discovered.energyManager ? this.discovered.energyManager : 'no');
			}
		}
	}.bind(this), 1000);
}

SMAHomeManager.prototype = {

	_setAccessoryInformation(accessory) {
		const serialNumbers = this.discovered.inverter.SerialNumber + ' & ' + this.discovered.energyManager.SerialNumber;
		const firmwareRevisions = this.discovered.inverter.FirmwareRevision + ' & ' + this.discovered.energyManager.FirmwareRevision;
		accessory.getService(Service.AccessoryInformation)
			// @see https://github.com/homebridge/HAP-NodeJS/issues/940#issuecomment-1111470278
			.setCharacteristic(Characteristic.Manufacturer, 'SMA Solar Technology AG')
			.setCharacteristic(Characteristic.Model, 'Sunny Boy & Home Manager 2.0')
			.setCharacteristic(Characteristic.SerialNumber, serialNumbers)
			.setCharacteristic(Characteristic.FirmwareRevision, firmwareRevisions);
	},

	accessories(callback) {
		this.live = new Accessory('Live power flow', Uuid.generate(PLATFORM + 'live'))
		const liveService = new Service.CustomPowerMonitor();
		liveService.addCharacteristic(Characteristic.CustomAmperes);
		liveService.addCharacteristic(Characteristic.CustomVolts);
		liveService.addCharacteristic(Characteristic.StatusActive);
		liveService.addCharacteristic(Characteristic.StatusFault);
		this.live.addService(liveService);

		this.recent = new Accessory('Recent power flow', Uuid.generate(PLATFORM + 'recent'))
		this.recent.addService(new Service.CustomPowerMonitor());

		this.today = new Accessory("Today's energy flow", Uuid.generate(PLATFORM + 'today'))
		this.today.addService(new Service.CustomEnergyMonitor());

		const signalNames = {
			offGrid: "Off Grid energy signal",
			noSun: "No Sun energy signal",
			highImport: "High Import energy signal",
		};
		Object.keys(signalNames).forEach(id => {
			if (!this.signalsConfig.builtIn[id]) {
				return;
			}
			const label = signalNames[id];
			this.signals[id] = new Accessory(label, Uuid.generate(PLATFORM + 'signals' + id));
			let createdService = new Service.CustomEnergySignal();
			if (id === 'highImport') {
				createdService.addCharacteristic(Characteristic.CustomImport);
			}
			this.signals[id].addService(createdService);
		});
		this.signalsConfig.surplus.forEach((signal, index) => {
			const id = 'surplus-' + signal.label.replace(' ', '-');
			this.signalsConfig.surplus[index].id = id;
			this.signals[id] = new Accessory(signal.label + ' energy signal', Uuid.generate(PLATFORM + 'signals' + id));
			this.signals[id].addService(new Service.CustomEnergySignal());
		});
		// TRICKY: for static platforms, this is apparently not provided by Homebridge 🤷‍♂️
		[this.live, this.recent, this.today, ...Object.values(this.signals)].forEach(accessory => {
			accessory.getServices = function() {
				return accessory.services;
			}
			// TRICKY: work around homebridge/homebridge#2815
			accessory.name = accessory.displayName;
		})

		// Store the callback; we'll call it after discovery finishes.
		// @see this.discovered
		this.accessoriesCallback = callback;
	},

	identify: function(callback) {
		this.log("identify");
		callback();
	},

	_connectToInverter: function() {
		this.log.debug("Connecting to inverter.");
		try {
			client.connectTCP(this.inverterAddress);
			client.setID(SMA_MODBUS_CLIENT_ID);
			this.log.debug("Successfully connected to inverter.");
		}
		catch(err) {
			this.log.error("Failed to connect to inverter.", err);
			return;
		}
	},

	_readInverterMetadata: function () {
			if (this.discovered.inverter) {
				return;
			}

			let serialNumber;
			let firmwareRevision;

			// Read serial number (U32, RAW).
			client.readHoldingRegisters(30057, 2, function(err, data) {
				serialNumber = data.buffer.readUInt32BE();
				if (firmwareRevision) {
					this.discovered.inverter = {
						SerialNumber: serialNumber,
						FirmwareRevision: firmwareRevision,
					};
				}
			}.bind(this));

			//  Read firmware version (U32, FW).
			client.readHoldingRegisters(40063, 2, function(err, data) {
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

	_readInverterData: function() {
		if (!this.launched) {
			return;
		}

		try {
			// Ensure this.computedToday remains up-to-date.
			const date = new Date();
			client.readHoldingRegisters(30581, 4).then((data) => {
				const currentTotals = {
					totalImport: data.buffer.slice(0, 4).readUInt32BE() / 1000, // Wh, U32, FIX0: 0 decimals
					totalExport: data.buffer.slice(4, 8).readUInt32BE() / 1000, // Wh, U32, FIX0: 0 decimals
				};
				if (this.computedToday.day != date.getDate()) {
					this.computedToday = {
						day: date.getDate(),
						// Note that we *could* in theory fall back to parsing https://169.254.12.3/dyn/getDashlogger.json … or SMA could just provide a proper API 🙃
						missedSeconds: (date.getHours() * 60 + date.getMinutes()) * 60 + date.getSeconds(),
						start: currentTotals,
						now: currentTotals,
					};
					this.log.info('New day! Retrieved total import & export at', date, this.computedToday);
					this.storage.setItemSync(PLUGIN_NAME + 'computedToday', this.computedToday);
				}
				else {
					this.computedToday.now = currentTotals;
				}
			});

			const inverter = this.live.getServiceById(Service.CustomPowerMonitor);

			// Inverter: StatusActive & StatusFault characteristics (U32, ENUM).
			client.readHoldingRegisters(30201, 2, function(err, data) {
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

			// Only when solar panels are currently producing can we set A & V.
			if (this.currentMeasurementIndex && this.measurements[this.currentMeasurementIndex].production > 0) {
				// Eve - Amperes (S32, FIX3: 3 decimals)
				client.readHoldingRegisters(30977, 2, function(err, data) {
					if (!data.buffer.equals(SMA_MODBUS_S32_NAN_VALUE)) {
						inverter.getCharacteristic(Characteristic.CustomAmperes).updateValue(data.buffer.readUInt32BE() / 1000);
					}
				}.bind(this));

				// Eve - Volts (U32, FIX2: 2 decimals).
				client.readHoldingRegisters(30783, 2, function(err, data) {
					if (!data.buffer.equals(SMA_MODBUS_U32_NAN_VALUE)) {
						inverter.getCharacteristic(Characteristic.CustomVolts).updateValue(data.buffer.readUInt32BE() / 100);
					}
				}.bind(this));
			}

			// Eve - kWh (Wh, U32, FIX0: 0 decimals)
			client.readHoldingRegisters(30535, 2, function(err, data) {
				let productionToday = 0;
				if (!data.buffer.equals(SMA_MODBUS_U32_NAN_VALUE)) {
					productionToday = data.buffer.readUInt32BE() / 1000;
				}

				if (this.computedToday.day == -1) {
					this.log.debug('Cannot update "Today" until necessary metadata is retrieved from inverter.')
					return;
				}

				// Compute today's numbers up to now.
				const exportToday = this.computedToday.now.totalExport - this.computedToday.start.totalExport;
				const importToday = this.computedToday.now.totalImport - this.computedToday.start.totalImport;

				// Update each of the 5 characteristics for "today".
				const t = {
					import: importToday,
					export: exportToday,
					production: productionToday,
					consumption: importToday + productionToday - exportToday,
				};
				const em = this.today.getServiceById(Service.CustomEnergyMonitor);
				em.getCharacteristic(Characteristic.CustomKilowattHours).updateValue(t.consumption);
				em.getCharacteristic(Characteristic.CustomKilowattHoursProduction).updateValue(t.production);
				em.getCharacteristic(Characteristic.CustomKilowattHoursImport).updateValue(t.import);
				em.getCharacteristic(Characteristic.CustomKilowattHoursExport).updateValue(t.export);
				em.getCharacteristic(Characteristic.CustomSelfSufficiency).updateValue(this._computeSelfSufficiencyLevel(t));
			}.bind(this));
		}
		catch(err) {
			this.log.error("Reading inverter data failed, will attempt to reconnect. Error:", err);
			this._connectToInverter();
		}
	},

	_listenToHomeManager: function() {
		this.socket = dgram.createSocket({type: 'udp4', reuseAddr: true});

		this.socket.on('error', function(err) {
			this.log.error("Listening to SMA Home Manager failed, will attempt to reconnect. Error:", err);
			this.importRealtime.getCharacteristic(Characteristic.On).updateValue(false);
			this.exportRealtime.getCharacteristic(Characteristic.On).updateValue(false);
			this.importService.getCharacteristic(Characteristic.On).updateValue(false);
			this.exportService.getCharacteristic(Characteristic.On).updateValue(false);
			this.clearInterval(this.multicastMembershipIntervalId);
			this.socket.close(function() {
				setTimeout(this._listenToHomeManager.bind(this), 10 * 1000)
			}.bind(this));
		}.bind(this));

		this.socket.on('listening', function() {
			this.socket.addMembership(this.homeManagerAddress);
			this.multicastMembershipIntervalId = setInterval(this._keepMembershipActive.bind(this), 120*1000);
		}.bind(this));

		this.socket.on('message', function(msg, rinfo) {
			if (!this._isValidDatagram(msg)) {
				return;
			}

			// When not yet launched, enable the launch to happen by only parsing the
			// datagram, this will populate this.discovered.energyManager.
			if (!this.launched) {
				this._parseDatagram(msg, rinfo);
				return;
			}

			// Read the live production from the inverter, to minimize the time offset
			// relative to the received energy manager datagram (S32, FIX0: 0 decimals).
			const before = performance.now();
			client.readHoldingRegisters(30775, 2).then((data) => {
				return data.buffer.equals(SMA_MODBUS_S32_NAN_VALUE) ? 0 : data.buffer.readInt32BE();
			})
			.then((producedWatts) => {
				const estimatedMsOffset = performance.now() - before;
				const [timestamp, netWatts] = this._parseDatagram(msg, rinfo);
				this._processMeasurement(producedWatts, netWatts, timestamp);
			})
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
			consumption: importWatts + producedWattsFromInverter - exportWatts,
		};

		// Store measurement and compute next measurement index.
		var currentIndex = this.nextMeasurementIndex;
		this.measurements[currentIndex] = measurement;
		this.currentMeasurementIndex = currentIndex;
		this.nextMeasurementIndex = (currentIndex + 1) % this.measurementsNeeded;

		// Provide a sequential view of the measurements, to faciliate recency metrics.
		// (The latest measurement is at the end.)
		const sequentialMeasurements = currentIndex === this.measurements.length - 1
			? this.measurements
			: this.measurements
				.slice(-this.measurements.length + currentIndex + 1)
				.concat(this.measurements.slice(0, currentIndex + 1));

		if (currentIndex === this.measurementsNeeded) {
			this.log.debug(
				'Periodic measurement assumptions check: average time between measurements should be 1 s, actually:',
				sequentialMeasurements
					.map(m => m.timestamp)
					.reduceRight((result, val, index, array) => {
						return (index == 0) ? result : result + val - array[index - 1];
					}, 0) / (sequentialMeasurements.length - 1)
			);
		}

		// Update each of the 5 characteristics for both "live" and "recent".
		const m = measurement;
		const pmLive = this.live.getServiceById(Service.CustomPowerMonitor);
		pmLive.getCharacteristic(Characteristic.CustomWatts).updateValue(m.consumption);
		pmLive.getCharacteristic(Characteristic.CustomProduction).updateValue(m.production);
		pmLive.getCharacteristic(Characteristic.CustomImport).updateValue(m.import);
		pmLive.getCharacteristic(Characteristic.CustomExport).updateValue(m.export);
		pmLive.getCharacteristic(Characteristic.CustomSelfSufficiency).updateValue(this._computeSelfSufficiencyLevel(m));
		const recentMeasurements = sequentialMeasurements.slice(-1 * this.recentMinutes * 60);
		let r = {};
		['import', 'export', 'production', 'consumption'].forEach(type => {
			r[type] = recentMeasurements.map(measurement => measurement[type]).reduce(this._reduceToAvg, 0);
		});
		const pmRecent = this.recent.getServiceById(Service.CustomPowerMonitor);
		pmRecent.getCharacteristic(Characteristic.CustomWatts).updateValue(r.consumption);
		pmRecent.getCharacteristic(Characteristic.CustomProduction).updateValue(r.production);
		pmRecent.getCharacteristic(Characteristic.CustomImport).updateValue(r.import);
		pmRecent.getCharacteristic(Characteristic.CustomExport).updateValue(r.export);
		pmRecent.getCharacteristic(Characteristic.CustomSelfSufficiency).updateValue(this._computeSelfSufficiencyLevel(r));

		// Update offGrid signal, if enabled.
		if (this.signals.offGrid) {
			const offGridSignal = this.signals.offGrid.getServiceById(Service.CustomEnergySignal);
			const offGridSeconds = sequentialMeasurements.length - 1 - sequentialMeasurements
				.map(m => m.import)
				.findLastIndex(w => w > 0);
			offGridSignal.getCharacteristic(Characteristic.On).updateValue(offGridSeconds >= 60);
			let offGridReason = 'Import > 0 W in past min.';
			if (offGridSeconds > 0 && offGridSeconds < 60) {
				offGridReason = 'Import = 0 W, but for < 1 min.';
			}
			else if (offGridSeconds >= 60) {
				offGridReason = `Import = 0 W for ≥ ${parseInt(offGridSeconds / 60)} mins.`;
			}
			offGridSignal.getCharacteristic(Characteristic.CustomReason).updateValue(offGridReason);
		}
		// Update noSun signal, if enabled.
		if (this.signals.noSun) {
			const noSunSignal = this.signals.noSun.getServiceById(Service.CustomEnergySignal);
			const noSunSeconds = sequentialMeasurements.length - 1 - sequentialMeasurements
				.map(m => m.production)
				.findLastIndex(w => w > 0);
			const producedSomeWattsToday = this.today.getServiceById(Service.CustomEnergyMonitor)
				.getCharacteristic(Characteristic.CustomKilowattHoursProduction).value > 0;
			noSunSignal.getCharacteristic(Characteristic.On).updateValue(noSunSeconds >= 900);
			// TRICKY: SMA decided to not populate ModBus registers 30199 ("waiting time
			// until feed-in"). Consequently, it is impossible to know how long until PV
			// production is expected to start (or end). It appears to be available via
			// the SMAData2 protocol, but supporting that introduces lots of complexity.
			// @todo Use the "suncalc" package to compute more sensible reasons before & after today's sunny period? If SMA exposed this information
			noSunSignal.getCharacteristic(Characteristic.CustomReason).updateValue(noSunSeconds > 0
				? (
					producedSomeWattsToday
					? `Production = 0 W for ${noSunSeconds >= 900 ? '≥' : '' + Math.round(noSunSeconds / 60) + ' <'} 15 mins.`
					: 'Awaiting first ray of sunlight…'
				)
				: 'Production > 0 W in past min.'
			);
		}
		// Update highImport signal, if enabled.
		if (this.signals.highImport) {
			const highImportSignal = this.signals.highImport.getServiceById(Service.CustomEnergySignal);
			const avgImportWattsLast15Min = sequentialMeasurements.slice(-15 * 60)
				.map(m => m.import)
				.reduce(this._reduceToAvg, 0);
			// When far below the 2500 W treshold, round to the nearest 100 W, to avoid highly frequent updates.
			highImportSignal.getCharacteristic(Characteristic.CustomImport).updateValue(avgImportWattsLast15Min < 2000
				? Math.round(avgImportWattsLast15Min / 100) * 100
				: avgImportWattsLast15Min
			);
			highImportSignal.getCharacteristic(Characteristic.On).updateValue(avgImportWattsLast15Min > 2500);
			highImportSignal.getCharacteristic(Characteristic.CustomReason).updateValue(`${Math.round(sequentialMeasurements.length / 60)} min mean import ≅ ${ Math.round(avgImportWattsLast15Min/100) / 10 } ${avgImportWattsLast15Min > 2500 ? '>' : '≤'} 2.5 kW.`);
		}

		let accumulatedSurplusWatts = 0;
		const surplusMeasurements = sequentialMeasurements.map(m => m.export);
		this.signalsConfig.surplus.forEach((signal, index) => {
			const samplesForSignal = signal.minutes * 60;
			const requiredWatts = signal.watts;
			const sortedSamplingWindow = surplusMeasurements.slice(-samplesForSignal).sort();
			const actualSamplesForSignal = sortedSamplingWindow.length;
			const service = this.signals[signal.id].getServiceById(Service.CustomEnergySignal);
			// Don't toggle the surplus signal unless there's actually enough samples.
			if (surplusMeasurements.length < samplesForSignal) {
				service.getCharacteristic(Characteristic.CustomReason).updateValue(`< ${signal.minutes} mins of data …`);
				return;
			}
			const min = sortedSamplingWindow.reduce((min, value) => Math.min(min, value), Infinity);
			const p90 = sortedSamplingWindow[Math.round(0.9 * actualSamplesForSignal) - 1];
			// In the past `signal.minutes` the minimum surplus should exceed
			// `signal.watts`, and 90% of the time it should also cover the base load
			// variability. (To avoid frequent toggling.)
			const hasSurplus = min > signal.watts && p90 > signal.watts + this.baseLoadVariability + accumulatedSurplusWatts;
			let reason = `Surplus ≤ ${signal.watts} W for ≥ ${signal.minutes} mins.`;
			if (!hasSurplus && min > signal.watts) {
				// Note: use of the "greater than or approximate" sign to succinctly indicate base load is not covered.
				reason = `Surplus ⪆ ${signal.watts} W for ≥ ${signal.minutes} mins.`;
			}
			else if (hasSurplus) {
				// Note: use the "much greater than" sign to succinctly indicate base load is also covered.
				reason = `Surplus ≫ ${signal.watts} W for ≥ ${signal.minutes} mins.`;
			}
			service.getCharacteristic(Characteristic.On).updateValue(hasSurplus);
			service.getCharacteristic(Characteristic.CustomReason).updateValue(reason);

			// Take the accumulated required surplus watts into account for the subsequent signals.
			accumulatedSurplusWatts += requiredWatts;
		});
	},

	_reduceToAvg: (avg, v, _, { length }) => avg + v / length,

	_computeSelfSufficiencyLevel: (m) => {
		// -100%: no production implies no self-sufficiency at all.
		if (m.production === 0) {
			return -100;
		}
		// 0–99%: production, import was needed, so 100% is impossible.
		if (m.import > 0) {
			return Math.min((m.production - m.export) / m.consumption * 100, 99);
		}
		// 100–1000%: production, no import. (Limit to 1000%: 10⨉ consumption!)
		return Math.min(m.production / m.consumption * 100, 10000);
	},

	// TRICKY: https://github.com/nodejs/node/issues/39377
	// TRICKY: https://datatracker.ietf.org/doc/html/rfc3376#section-8.2
	_keepMembershipActive: function() {
		this.log.debug('Dropping and re-adding multicast membership');
		this.socket.dropMembership(this.homeManagerAddress);
		this.socket.addMembership(this.homeManagerAddress);
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
	}

};
