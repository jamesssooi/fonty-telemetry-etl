require('module-alias/register');
require('source-map-support').install();
const start = require('./build/app.js').default;
start();