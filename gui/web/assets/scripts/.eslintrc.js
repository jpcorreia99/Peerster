// npm install eslint --save-dev
// npx eslint --fix main.js
module.exports = {
    "env": {
        "browser": true,
        "es2021": true
    },
    "extends": "eslint:recommended",
    "parserOptions": {
        "ecmaVersion": 12
    },
    "rules": {
        "indent": ["error", 4],
        "no-trailing-spaces": ["error", {}],
        "semi": ["error", "always"]
    }
};
