#rm -f *.jks
users="$1"
keytool -genkey -alias "${users}" -keyalg RSA -validity 365 -keystore ./"${users}".jks -storetype pkcs12 -ext SAN=dns:"${users}"<<EOF
123users
123users
${users}
TP2
SD2223
LX
LX
PT
yes
123users
123users
EOF

echo
echo
echo "Exporting Certificates"
echo
echo

keytool -exportcert -alias "${users}" -keystore "${users}".jks -file "${users}".cert -ext SAN=dns:"${users}" <<EOF
123users
EOF


echo "Creating Client Truststore"
if ! [[ -f client-ts.jks ]]; then
  cp cacerts client-ts.jks
fi
keytool -importcert -file "${users}".cert -alias "${users}" -keystore client-ts.jks <<EOF
changeit
yes
EOF
