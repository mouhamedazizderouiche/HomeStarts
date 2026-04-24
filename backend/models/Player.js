const mongoose = require("mongoose");

const PlayerSchema = new mongoose.Schema(
  {
    api_football_id: { type: String, default: "", trim: true },
    transfermarkt_id: { type: String, required: true },
    id: { type: String, required: true },
    name: { type: String, required: true, trim: true },
    nationality: { type: String, required: true, trim: true },
    nationality_code: { type: String, default: "", trim: true },
    club: { type: String, required: true, trim: true },
    current_club: { type: String, default: "", trim: true },
    league: { type: String, default: "", trim: true },
    position: { type: String, default: "Unknown", trim: true },
    date_of_birth: { type: Date, default: null },
    market_value: { type: Number, default: null },
    team_id: { type: String, default: "", trim: true },
    image: { type: String, default: "", trim: true },
    image_url: { type: String, default: "", trim: true },
    flag: { type: String, default: "", trim: true },
    source: { type: String, default: "transfermarkt", trim: true },
    sources: { type: [String], default: [] },
    data_sources: { type: [String], default: [] },
    is_active: { type: Boolean, default: true, index: true },
    player_status: {
      type: String,
      default: "active",
      enum: ["verified", "active", "uncertain"],
      index: true
    },
    rating_2025: { type: Number, default: 0 },
    form_score: { type: Number, default: 0, index: true },
    top_score: { type: Number, default: 0, index: true },
    last_seen_at: { type: Date, default: null, index: true },
    last_match_date: { type: Date, default: null, index: true },
    dual_nationality: { type: [String], default: [] },
    origin_countries: { type: [String], default: [] },
    last_season: { type: Number, default: null },
    minutes_played: { type: Number, default: 0 },
    matches_2025: { type: Number, default: 0 },
    goals_2025: { type: Number, default: 0 },
    assists_2025: { type: Number, default: 0 },
    age: { type: Number, default: null },
    normalized_name: { type: String, required: true, trim: true },
    updatedAt: { type: Date, default: Date.now }
  },
  {
    versionKey: false
  }
);

PlayerSchema.index({ nationality: 1 });
PlayerSchema.index({ nationality: 1, is_active: 1 });
PlayerSchema.index({ name: 1 });
PlayerSchema.index({ transfermarkt_id: 1 }, { unique: true });
PlayerSchema.index({ api_football_id: 1 });
PlayerSchema.index({ normalized_name: 1 });
PlayerSchema.index({ normalized_name: 1, date_of_birth: 1 });

module.exports = mongoose.model("Player", PlayerSchema);
