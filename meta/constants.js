

/**
 *
 */
module.exports.STANDARD_IDENTITY = {

  // Friend.
  // A BSO that belongs to a declared friendly nation.
  'FR': 'F',

  // Assumed friend.
  // A BSO that is assumed to be a friend because
  // of its characteristics, behaviour or origin.
  'AFR': 'A',

  // Neutral.
  // A BSO whose characteristics, behaviour, origin or
  // nationality indicate that it is neither supporting
  // friendly nor opposing forces.
  'NEUTRL': 'N',

  // Faker.
  // A BSO that is a friendly aircraft simulating a
  // hostile aircraft in an air defence exercise.
  'FAKER': 'K',

  // Hostile.
  // A BSO that is positively identified as enemy.
  'HO': 'H',

  // Joker.
  // A BSO that is acting as a suspect track for
  // exercise purposes only.
  'JOKER': 'J',

  // Assumed hostile.
  // An indication that the BSO in question is
  // likely to belong to enemy forces.
  'AHO': 'S',

  // Suspect.
  // A BSO that is potentially hostile because of
  // its characteristics, behaviour or origin.
  'SUSPCT': 'S',

  // Pending.
  // A BSO for which identification is to be determined.
  'PENDNG': 'P',

  // Assumed neutral.
  // An indication that the BSO in question is likely
  // to belong to neither own, allied, enemy or otherwise
  // involved forces.
  'ANT': 'P',

  // Involved.
  // An indication that the BSO in question belongs
  // to involved forces different from own, allied and enemy forces.
  'IV': 'P',

  // Assumed involved.
  // An indication that the BSO in question is likely
  // to belong to involved forces different from own,
  // allied and enemy forces.
  'AIV': 'P',

  // Unknown.
  // A BSO for which its hostility status information
  // is not available.
  'UNK': 'U'
}


/**
 *
 */
module.exports.SIZE_CODE = {

  // Team/fire team/crew. Ø
  // Any unit smaller than a squad that will be
  // denoted by a vehicle or weapon symbol in a
  // graphical representation.
  'TEAM': 'A',

  // Squad. •
  // A small number of men, a subdivision or section
  // of a company, formed for drill.
  // 5–10, 1–2 fireteams
  'SQUAD': 'B',

  // Section. ••
  // A small tactical unit.
  // 7–13, 2–3 fireteams
  'SECT': 'C',

  // Platoon. •••
  // Basic administrative and tactical unit
  // in most arms and services of the Army.
  // 25–40, Several squads, sections, or vehicles
  'PLT': 'D',

  // Company. I
  // Basic administrative and tactical unit in
  // most arms and services of the Army.
  // A company is on a command level below a battalion
  // and above a platoon.
  // 60–250
  'COY': 'E',

  // Battalion. II
  // Unit composed of a headquarters and two or
  // more companies or batteries. It may be part of a
  // regiment and be charged with only tactical functions,
  // or it may be a separate unit and be charged with both
  // administrative and tactical functions.
  // 300–1,000
  'BN': 'F',

  // Squadron, air.
  // An administrative or tactical organisation normally
  // but not necessarily composed of aircraft of the same type.
  'SQDRNA': 'F',

  // Squadron, maritime.
  // An administrative or tactical organisation consisting
  // of two or more divisions of ships, plus such additional
  // ships as may be assigned as flagships or tenders.
  'SQDRNM': 'F',

  // Regiment. III
  // Administrative and tactical unit, on a command level
  // below a division or brigade and above a battalion,
  // the entire organisation of which is prescribed by
  // table of organisation.
  // 500–2,000
  'RGT': 'G',

  // Brigade. X
  // Unit composed of a headquarters and two or more
  // battalions. It may be part of an army and be
  // charged with only tactical functions, or it may be a
  // separate unit and be charged with both administrative
  // and tactical functions.
  // 2,000–5,000 U.S (USMC MEB 7,000–20,000),
  'BDE': 'H',

  // Brigade group.
  // An operational grouping which is based on an infantry
  // or armoured brigade and which has elements of other supporting
  // arms and services allocated according to need.
  'BDEGRP': 'H',

  // Division. XX
  // A tactical unit/formation that is a major administrative
  // and tactical unit/formation that combines in itself the
  // necessary arms and services required for sustained combat,
  // larger than a regiment/brigade and smaller than a corps.
  // 10,000–20,000
  'DIV': 'I',

  // Corps. XXX
  // A formation larger than a division but smaller than an
  // army or army group. It usually consists to two or more
  // divisions together with supporting arms and services.
  // 30,000–60,000 (USMC MEF 20,000–90,000)
  'CORPS': 'J',

  // Army. XXXX
  // A formation larger than an army corps but
  // smaller than an army group. It usually consists
  // of two or more army corps.
  // 100,000
  'ARMY': 'K',

  // Army group: XXXXX
  // The largest formation of land forces,
  // normally comprising two or more armies or
  // army corps under a designated commander.
  // 120,000–500,000
  'AG': 'L',

  // Region: XXXXXX
  // A grouping of two or more Army groups.
  // 250,000–1,000,000+
  'REGION': 'M'
}

module.exports.STATUS = {
  ASS: 'A',
  PRDCTD: 'A',
  PLAN: 'A',
  REP: 'P',
  INFER: 'P'
}