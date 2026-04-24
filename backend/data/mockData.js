const players = [
  {
    id: "tun-1",
    name: "Hannibal Mejbri",
    country: "Tunisia",
    club: "Sevilla FC",
    position: "Midfielder",
    age: 23,
    photo:
      "https://images.unsplash.com/photo-1612872087720-bb876e2e67d1?auto=format&fit=crop&w=300&q=80",
    matches: [
      {
        id: "m1",
        opponent: "Real Betis",
        date: "2026-04-22",
        competition: "LaLiga"
      },
      {
        id: "m2",
        opponent: "Valencia CF",
        date: "2026-04-27",
        competition: "LaLiga"
      }
    ]
  },
  {
    id: "tun-2",
    name: "Ellyes Skhiri",
    country: "Tunisia",
    club: "Eintracht Frankfurt",
    position: "Defensive Midfielder",
    age: 31,
    photo:
      "https://images.unsplash.com/photo-1517466787929-bc90951d0974?auto=format&fit=crop&w=300&q=80",
    matches: [
      {
        id: "m3",
        opponent: "Bayer Leverkusen",
        date: "2026-04-21",
        competition: "Bundesliga"
      },
      {
        id: "m4",
        opponent: "Mainz 05",
        date: "2026-04-29",
        competition: "Bundesliga"
      }
    ]
  },
  {
    id: "mar-1",
    name: "Achraf Hakimi",
    country: "Morocco",
    club: "Paris Saint-Germain",
    position: "Right Back",
    age: 27,
    photo:
      "https://images.unsplash.com/photo-1570498839593-e565b39455fc?auto=format&fit=crop&w=300&q=80",
    matches: [
      {
        id: "m5",
        opponent: "Olympique Lyon",
        date: "2026-04-23",
        competition: "Ligue 1"
      },
      {
        id: "m6",
        opponent: "AS Monaco",
        date: "2026-05-01",
        competition: "Ligue 1"
      }
    ]
  },
  {
    id: "mar-2",
    name: "Youssef En-Nesyri",
    country: "Morocco",
    club: "Fenerbahce",
    position: "Striker",
    age: 29,
    photo:
      "https://images.unsplash.com/photo-1628891890467-b79cdd4e48ff?auto=format&fit=crop&w=300&q=80",
    matches: [
      {
        id: "m7",
        opponent: "Galatasaray",
        date: "2026-04-24",
        competition: "Super Lig"
      },
      {
        id: "m8",
        opponent: "Besiktas",
        date: "2026-05-03",
        competition: "Super Lig"
      }
    ]
  },
  {
    id: "fra-1",
    name: "Kylian Mbappe",
    country: "France",
    club: "Real Madrid",
    position: "Forward",
    age: 27,
    photo:
      "https://images.unsplash.com/photo-1624880357913-a8539238245b?auto=format&fit=crop&w=300&q=80",
    matches: [
      {
        id: "m9",
        opponent: "FC Barcelona",
        date: "2026-04-26",
        competition: "LaLiga"
      },
      {
        id: "m10",
        opponent: "Atletico Madrid",
        date: "2026-05-02",
        competition: "LaLiga"
      }
    ]
  },
  {
    id: "fra-2",
    name: "Aurelien Tchouameni",
    country: "France",
    club: "Real Madrid",
    position: "Midfielder",
    age: 26,
    photo:
      "https://images.unsplash.com/photo-1546519638-68e109498ffc?auto=format&fit=crop&w=300&q=80",
    matches: [
      {
        id: "m11",
        opponent: "Athletic Club",
        date: "2026-04-20",
        competition: "LaLiga"
      },
      {
        id: "m12",
        opponent: "Sevilla FC",
        date: "2026-04-30",
        competition: "LaLiga"
      }
    ]
  },
  {
    id: "arg-1",
    name: "Lautaro Martinez",
    country: "Argentina",
    club: "Inter Milan",
    position: "Striker",
    age: 29,
    photo:
      "https://images.unsplash.com/photo-1551958219-acbc608c6377?auto=format&fit=crop&w=300&q=80",
    matches: [
      {
        id: "m13",
        opponent: "AC Milan",
        date: "2026-04-25",
        competition: "Serie A"
      },
      {
        id: "m14",
        opponent: "Napoli",
        date: "2026-05-04",
        competition: "Serie A"
      }
    ]
  },
  {
    id: "arg-2",
    name: "Enzo Fernandez",
    country: "Argentina",
    club: "Chelsea",
    position: "Midfielder",
    age: 25,
    photo:
      "https://images.unsplash.com/photo-1508098682722-e99c43a406b2?auto=format&fit=crop&w=300&q=80",
    matches: [
      {
        id: "m15",
        opponent: "Arsenal",
        date: "2026-04-22",
        competition: "Premier League"
      },
      {
        id: "m16",
        opponent: "Liverpool",
        date: "2026-05-01",
        competition: "Premier League"
      }
    ]
  }
];

const countries = [
  { name: "Tunisia", flag: "\uD83C\uDDF9\uD83C\uDDF3" },
  { name: "Morocco", flag: "\uD83C\uDDF2\uD83C\uDDE6" },
  { name: "France", flag: "\uD83C\uDDEB\uD83C\uDDF7" },
  { name: "Argentina", flag: "\uD83C\uDDE6\uD83C\uDDF7" }
];

module.exports = {
  players,
  countries
};
