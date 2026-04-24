function SearchBar({ value, onChange, placeholder = "Search countries..." }) {
  return (
    <div
      className="w-full rounded-2xl border p-1 backdrop-blur-sm transition-all duration-200 focus-within:shadow-neon"
      style={{
        background: "rgba(255,255,255,0.05)",
        borderColor: "rgba(255,255,255,0.1)",
        boxShadow: "0 0 20px rgba(0,255,65,0.16)"
      }}
    >
      <input
        type="text"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        className="w-full rounded-xl border border-transparent bg-slate-950/70 px-4 py-3 text-sm text-[#E2E8F0] placeholder:text-slate-400 focus:border-[#00FF41] focus:outline-none"
      />
    </div>
  );
}

export default SearchBar;
