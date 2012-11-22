if [[ ! -o interactive ]]; then
    return
fi

compctl -K _novac novac

_novac() {
  local word words completions
  read -cA words
  word="${words[2]}"

  if [ "${#words}" -eq 2 ]; then
    completions="$(novac commands)"
  else
    completions="$(novac completions "${word}")"
  fi

  reply=("${(ps:\n:)completions}")
}
