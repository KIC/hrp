// just out of curiosity, try to regress the price changes of past price changs and try to find a drift from one coin
// to another like in votings
// SPÖ2013 = b1 × SPÖ2008 + b2 × ÖVP2008 + b3 × FPÖ2008 + b4 × BZÖ2008 + b5 x Grüne2008 + b6 × Sonstige2008 + b7 × NichtwählerInnen2008
// Price-BTC-t = b1 * Price-BTC-t-1 + b2 * Price-XMR-t-1 + ....  bn * price-COIN(n)-t-1 ... + d => then do Ax = b (solve for x)
// repeang this for all coins -> this tells us (in theory) how much dollars are moving from one coin to anotherone
// http://derstandard.at/2000035850803/Wie-Waehlerstromanalysen-funktionieren?_blogGroup=1
// idealy we constrain b to be: 0 <= b <= 1 I am pretty confident we jan use the joptimzer solvers for this issue.
// also the marketcap-of-t - marketcap-of-t-1 could somehow represent the "nicht wähler" repectivly the "gesamt wähler" or "andere"
// the question is if we could get smooth koefficients over t or if they are jumping just heavily from one day to another
// see also: http://derstandard.at/2000035850803/Wie-Waehlerstromanalysen-funktionieren
