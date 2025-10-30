Pod::Spec.new do |s|
  s.name = 'Jmmartin81CapacitorHealth'
  s.version = '0.0.1'
  s.summary = 'Capacitor plugin for Apple HealthKit and Android Health Connect'
  s.license = 'MIT'
  s.homepage = 'https://github.com/jmmartin81/capacitor-health'
  s.author = 'Jmmartin81'
  s.source = { :path => '.' }
  s.source_files = 'ios/Sources/**/*.{swift,h,m}'
  s.dependency 'Capacitor'
  s.swift_version = '5.0'
  s.platform = :ios, '13.0'
end
