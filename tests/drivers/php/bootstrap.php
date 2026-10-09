<?php

$loader = require __DIR__ . '/vendor/autoload.php';
$loader->addPsr4('Serenedb\\Drivers\\', __DIR__ . '/src/');
$loader->addPsr4('Serenedb\\Drivers\\Tests\\', __DIR__ . '/tests/');
