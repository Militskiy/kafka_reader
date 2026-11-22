//! Desktop language selection. The saved preference is independent of the current OS language.
mod messages;

use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LanguagePreference {
    #[default]
    System,
    English,
    Russian,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Language {
    English,
    Russian,
}

impl LanguagePreference {
    pub fn resolve(self) -> Language {
        static SYSTEM: OnceLock<Language> = OnceLock::new();
        match self {
            Self::System => *SYSTEM.get_or_init(system_language),
            Self::English => Language::English,
            Self::Russian => Language::Russian,
        }
    }
}

impl Language {
    pub fn text<'a>(self, english: &'a str, russian: &'a str) -> &'a str {
        match self {
            Self::English => english,
            Self::Russian => russian,
        }
    }
}

#[cfg(windows)]
fn system_language() -> Language {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetUserDefaultUILanguage() -> u16;
    }
    // SAFETY: this argument-free Win32 function returns the user's display LANGID.
    from_windows_langid(unsafe { GetUserDefaultUILanguage() })
}

#[cfg(any(windows, test))]
fn from_windows_langid(langid: u16) -> Language {
    // PRIMARYLANGID uses the low ten bits. LANG_RUSSIAN is 0x19.
    if langid & 0x03ff == 0x19 {
        Language::Russian
    } else {
        Language::English
    }
}

#[cfg(not(windows))]
fn system_language() -> Language {
    let message_locale = ["LC_ALL", "LC_MESSAGES", "LANG"]
        .into_iter()
        .filter_map(|key| std::env::var(key).ok())
        .find(|value| !value.is_empty());
    posix_language(
        message_locale.as_deref(),
        std::env::var("LANGUAGE").ok().as_deref(),
    )
}

#[cfg(any(not(windows), test))]
fn posix_language(locale: Option<&str>, preferences: Option<&str>) -> Language {
    let locale = locale.unwrap_or("C");
    if matches!(locale, "C" | "POSIX") {
        return Language::English;
    }
    for name in preferences
        .into_iter()
        .flat_map(|list| list.split(':'))
        .chain([locale])
    {
        let tag = name.trim().split(['-', '_', '.', '@']).next().unwrap_or("");
        if tag.eq_ignore_ascii_case("ru") {
            return Language::Russian;
        }
        if tag.eq_ignore_ascii_case("en") || tag == "C" || tag == "POSIX" {
            return Language::English;
        }
    }
    Language::English
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::Workspace;

    #[test]
    fn display_language_and_posix_message_locales() {
        assert_eq!(from_windows_langid(0x0419), Language::Russian);
        assert_eq!(from_windows_langid(0x0409), Language::English);
        assert_eq!(from_windows_langid(0x0422), Language::English);
        assert_eq!(posix_language(Some("ru_RU.UTF-8"), None), Language::Russian);
        assert_eq!(posix_language(Some("RU-KZ"), None), Language::Russian);
        assert_eq!(
            posix_language(Some("de_DE"), Some("de:ru:en")),
            Language::Russian
        );
        assert_eq!(
            posix_language(Some("ru_RU"), Some("en:ru")),
            Language::English
        );
        assert_eq!(posix_language(Some("C"), Some("ru")), Language::English);
        assert_eq!(posix_language(Some("fr_FR"), None), Language::English);
    }

    #[test]
    fn legacy_workspace_follows_system_and_explicit_choices_roundtrip() {
        let mut workspace: Workspace =
            serde_json::from_str(r#"{"version":2,"settings":{"font_size":17}}"#).unwrap();
        assert_eq!(workspace.settings.font_size, 17.);
        assert_eq!(workspace.settings.language, LanguagePreference::System);
        for (choice, language) in [
            (LanguagePreference::Russian, Language::Russian),
            (LanguagePreference::English, Language::English),
        ] {
            workspace.settings.language = choice;
            let reopened: Workspace =
                serde_json::from_slice(&serde_json::to_vec(&workspace).unwrap()).unwrap();
            assert_eq!(reopened.settings.language, choice);
            assert_eq!(reopened.settings.language.resolve(), language);
        }
    }
}
